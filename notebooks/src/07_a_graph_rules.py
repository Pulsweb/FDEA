# %% [markdown]
# # 7. Migration readiness analysis
# The analysis is a **pure function of the Silver tables** (`run_analysis`): it never calls an API, so it can be re-run
# alone (`p_run_mode = "analysis_only"`) after tuning rules, weights or governance overrides.
#
# ```
# Silver rows -> lineage graph -> rule evaluation -> per-path status (Wizard / Save-As) -> readiness class
#             -> complexity score (6 components) -> effort -> recommended path -> waves (topological) -> Gold rows
# ```
#
# ## 7.1 Readiness precedence
# | # | Condition | Class |
# |---|---|---|
# | 1 | Governance override `defer` | **Deferred** (override) |
# | 2 | Stale (no successful refresh for `p_stale_days`) **and** no downstream consumers | **Deferred** - decommission candidate |
# | 3 | A rule is *Unsupported* on every available path | **Unsupported** |
# | 4 | `p_failing_consecutive` consecutive refresh failures | **Deferred** - fix first |
# | 5 | An upstream dataflow is Unsupported or Deferred | **Deferred** - blocked by upstream (SEQ-001, propagated transitively) |
# | 6 | A rule needs attention on the best available path (or the definition could not be inspected) | **Needs Attention** |
# | 7 | otherwise | **Ready** |
#
# Dataflows that are already Gen2 are out of scope (`readiness = NULL`).

# %%
MAX_DEPTH = 6
MAX_DEPENDENCY_ROWS = 2000
DEFAULT_WEIGHTS = {"dependencies": 20, "refresh_criticality": 15, "gateway": 15, "source_complexity": 15, "unsupported": 25, "technical": 10}
BAND_THRESHOLDS = [(75, "Very High"), (50, "High"), (25, "Medium"), (0, "Low")]
BASE_EFFORT_DAYS = {"Low": 0.5, "Medium": 1.5, "High": 4.0, "Very High": 8.0}
WAVE_LABELS = {0: "Wave 0 - Pilot", 1: "Wave 1 - Quick wins", 2: "Wave 2 - Standard", 3: "Wave 3 - Complex or critical",
               4: "Wave 4 - Remediate or rebuild", 99: "Backlog - Deferred"}
STATUS_ORDER = {"Ready": 0, "NeedsAttention": 1, "Unsupported": 2}
STATUS_LABEL = {"Ready": "Ready", "NeedsAttention": "Needs Attention", "Unsupported": "Unsupported"}
WIZARD_NAME = {"Ready": "Ready to migrate", "NeedsAttention": "Needs Attention", "Unsupported": "Upgrade unavailable"}
SEVERITY_ORDER = {"Blocker": 0, "Major": 1, "Minor": 2, "Info": 3}
REBIND_TYPES = ("Dataflow", "SemanticModel", "Datamart", "DataPipeline")


def compute_engine_state(value):
    """Normalize the undocumented computeEngineBehavior setting to On / Optimized / Off / Unknown."""
    text = re.sub(r"[^a-z]", "", str(value or "").lower())
    if not text or text == "notconfigured":
        return "Unknown"
    if "optimiz" in text:
        return "Optimized"
    if "disabl" in text or text.endswith("off"):
        return "Off"
    if text.endswith("on") or "enabl" in text or "ondemand" in text:
        return "On"
    return "Unknown"


def json_object(value):
    """A dict from a JSON text (or a dict). null, lists, scalars and invalid text all become an empty dict."""
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def edge_sort_key(e):
    return (e["src_id"], e["dst_id"], str(e.get("relationship")), str(e.get("evidence")))


def dedupe_edges(edges):
    """Collapse edges seen by several sources into one per (src, dst, relationship); keep evidences and merged attributes.
    Edges without a source or a target id cannot be placed in the graph and are skipped. Input order never changes the result."""
    merged = {}
    for e in sorted((e for e in edges if e.get("src_id") and e.get("dst_id")), key=edge_sort_key):
        key = (e["src_id"], e["dst_id"], e.get("relationship"))
        attrs = json_object(e.get("attributes"))
        cur = merged.get(key)
        if cur is None:
            merged[key] = {**e, "evidences": [e.get("evidence")], "attrs": dict(attrs)}
        else:
            cur["evidences"].append(e.get("evidence"))
            cur["attrs"].update(attrs)
            for field in ("src_name", "dst_name", "src_workspace_id", "dst_workspace_id"):
                cur[field] = cur.get(field) or e.get(field)
    return list(merged.values())


class LineageGraph:
    """Impact graph (edge src -> dst: a change to src impacts dst) with bounded closure, SCCs and dependency levels."""

    def __init__(self, edges):
        self.edges = sorted(edges, key=edge_sort_key)
        self.down, self.up = defaultdict(list), defaultdict(list)
        self.nodes = {}
        for e in self.edges:
            self.down[e["src_id"]].append(e)
            self.up[e["dst_id"]].append(e)
            self.nodes.setdefault(e["src_id"], {"type": e.get("src_type"), "name": e.get("src_name"), "workspace_id": e.get("src_workspace_id")})
            self.nodes.setdefault(e["dst_id"], {"type": e.get("dst_type"), "name": e.get("dst_name"), "workspace_id": e.get("dst_workspace_id")})
            for end, nid in (("src", e["src_id"]), ("dst", e["dst_id"])):
                node = self.nodes[nid]
                node["name"] = node.get("name") or e.get(f"{end}_name")
                node["workspace_id"] = node.get("workspace_id") or e.get(f"{end}_workspace_id")

    def closure_info(self, anchor, direction, max_depth=MAX_DEPTH, limit=None):
        """BFS from anchor: ([(node_id, hop, edge, path_ids), ...] nearest first, truncated).
        With a limit, `truncated` is True only when more nodes were reachable than returned. Datasource nodes are leaves."""
        adjacency = self.down if direction == "DOWNSTREAM" else self.up
        seen, queue, out = {anchor}, deque([(anchor, 0, (anchor,))]), []
        while queue:
            node, hop, path = queue.popleft()
            if hop >= max_depth:
                continue
            for edge in adjacency.get(node, ()):
                nxt = edge["dst_id"] if direction == "DOWNSTREAM" else edge["src_id"]
                if nxt in seen:
                    continue
                if limit is not None and len(out) >= limit:
                    return out, True
                seen.add(nxt)
                out.append((nxt, hop + 1, edge, path + (nxt,)))
                if self.nodes.get(nxt, {}).get("type") != "Datasource":
                    queue.append((nxt, hop + 1, path + (nxt,)))
        return out, False

    def closure(self, anchor, direction, max_depth=MAX_DEPTH, limit=None):
        return self.closure_info(anchor, direction, max_depth, limit)[0]


def strongly_connected_components(nodes, successors):
    """Iterative Tarjan. Returns components in topological order of the condensation (upstream components first)."""
    index, low, on_stack, stack, comps, counter = {}, {}, set(), [], [], [0]
    for root in nodes:
        if root in index:
            continue
        work = [(root, iter(sorted(successors.get(root, ()))))]
        index[root] = low[root] = counter[0]
        counter[0] += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, it = work[-1]
            advanced = False
            for nxt in it:
                if nxt not in index:
                    index[nxt] = low[nxt] = counter[0]
                    counter[0] += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(sorted(successors.get(nxt, ())))))
                    advanced = True
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index[nxt])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                comp = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    comp.append(member)
                    if member == node:
                        break
                comps.append(sorted(comp))
    comps.reverse()
    return comps


def weak_components(nodes, adjacency):
    parent = {n: n for n in nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, targets in adjacency.items():
        for b in targets:
            if a in parent and b in parent:
                parent[find(a)] = find(b)
    groups = defaultdict(list)
    for n in nodes:
        groups[find(n)].append(n)
    return list(groups.values())

# %%
def bucket(value, steps):
    """Step function: the score of the highest threshold reached. steps = [(threshold, score), ...] ascending."""
    score = 0
    for threshold, points in steps:
        if value is not None and value >= threshold:
            score = points
    return score


def _label(node_id, graph, dfs):
    row = dfs.get(node_id)
    return (row or {}).get("dataflow_name") or graph.nodes.get(node_id, {}).get("name") or node_id


def _unique_by(edges, key):
    """One edge per related node (edges can arrive through several evidences); attributes are merged."""
    unique = {}
    for e in edges:
        cur = unique.get(e[key])
        if cur is None:
            unique[e[key]] = {**e, "attrs": dict(e["attrs"])}
        else:
            cur["attrs"].update(e["attrs"])
    return list(unique.values())


def direct_facts(df, graph, dfs):
    """Direct and transitive lineage facts used by rules, scores and criticality."""
    did = df["dataflow_id"]
    up_direct = _unique_by([e for e in graph.up.get(did, []) if e["src_type"] == "Dataflow"], "src_id")
    down_direct = _unique_by(graph.down.get(did, []), "dst_id")
    closure = graph.closure(did, "DOWNSTREAM")
    by_type = defaultdict(set)
    for nid, hop, edge, path in closure:
        by_type[graph.nodes.get(nid, {}).get("type", "Other")].add(nid)
    direct_types = defaultdict(list)
    for e in down_direct:
        direct_types[e["dst_type"]].append(e)
    wid = df.get("workspace_id")
    crosses = (any(e.get("src_workspace_id") and e["src_workspace_id"] != wid for e in up_direct)
               or any(e.get("dst_workspace_id") and e["dst_workspace_id"] != wid for e in down_direct))
    return {
        "upstream_dataflows": up_direct, "downstream_direct": down_direct, "direct_by_type": direct_types, "crosses_workspace": crosses,
        "downstream_dataflows": direct_types.get("Dataflow", []), "datasets_all": by_type.get("SemanticModel", set()),
        "reports_all": by_type.get("Report", set()), "dashboards_all": by_type.get("Dashboard", set()),
        "other_all": {n for t, s in by_type.items() if t not in ("SemanticModel", "Report", "Dashboard", "Dataflow") for n in s},
        "total_downstream": len(closure),
        "rebind_items": [e for t in REBIND_TYPES for e in direct_types.get(t, [])],
    }


def infer_criticality(df, facts, env):
    points = 0
    points += 2 if env == "Prod" else 0
    points += {"Certified": 2, "Promoted": 1, "Master data": 2}.get(df.get("endorsement"), 0)
    datasets = len(facts["datasets_all"])
    points += 2 if datasets >= 5 else (1 if datasets >= 1 else 0)
    points += 1 if facts["dashboards_all"] else 0
    points += 1 if len(facts["reports_all"]) >= 10 else 0
    points += 1 if (df.get("schedule_runs_per_day") or 0) >= 4 else 0
    return "High" if points >= 4 else ("Medium" if points >= 2 else "Low")


def make_hit(rule_id, entity="", evidence="", count=1):
    return {"rule_id": rule_id, "entity_name": entity or "", "evidence": evidence, "count": count}


def finding_names(findings, key):
    value = findings.get(key)
    return [str(v) for v in value] if isinstance(value, list) else []


def source_label(s):
    where = s.get("server") or s.get("url") or s.get("path") or s.get("datasource_id") or "?"
    return f"{s.get('datasource_type') or 'Source'}: {where}" + (f" / {s['database']}" if s.get("database") else "")


def evaluate_rules(cfg, now, df, ws, sources, facts, graph, dfs, same_name_count):
    """Rule hits for one Gen1 dataflow. Deferred-type rules (REF-001, STA-001) are included; SEQ-001 is added after propagation."""
    hits = []
    ws = ws or {}
    findings = json_object(df.get("definition_findings"))
    if ws.get("is_fabric_capable") is False or ws.get("is_on_dedicated_capacity") is False or ws.get("capacity_resolution") == "Shared":
        hits.append(make_hit("CAP-001", evidence=f"Workspace capacity: {ws.get('capacity_sku') or 'shared (Pro/PPU)'}"))
    if ws.get("capacity_resolution") == "Unresolved":
        hits.append(make_hit("CAP-002", evidence=f"capacity {ws.get('capacity_id')} not returned by the capacity APIs"))
    if df.get("storage_type") == "CustomADLS" or ws.get("dataflow_storage_id"):
        hits.append(make_hit("STO-001", evidence=f"modelUrl={df.get('model_url')}" if df.get("storage_type") == "CustomADLS" else f"workspace storage {ws.get('dataflow_storage_id')}"))
    ece = compute_engine_state(df.get("compute_engine_behavior"))
    if ece == "On":
        hits.append(make_hit("ECE-001", evidence=f"computeEngineBehavior={df.get('compute_engine_behavior')}"))
    elif ece == "Optimized":
        hits.append(make_hit("ECE-002", evidence="computeEngineBehavior=Optimized"))
    for e in facts["direct_by_type"].get("SemanticModel", []):
        if str(e["attrs"].get("storage_mode", "")).lower() == "directquery":
            hits.append(make_hit("DQ-001", e.get("dst_name") or e["dst_id"], "semantic model storage mode DirectQuery"))
    incremental = finding_names(findings, "incremental_entities")
    for name in incremental[:25]:
        hits.append(make_hit("INC-001", name, "incremental refresh policy on the entity"))
    if df.get("incremental_refresh_count") and not incremental:
        hits.append(make_hit("INC-001", "", f"{df['incremental_refresh_count']} entities with incremental refresh"))
    upstream_names = sorted({_label(e["src_id"], graph, dfs) for e in facts["upstream_dataflows"]})
    if (df.get("linked_entity_count") or 0) > 0 or upstream_names:
        hits.append(make_hit("LNK-001", evidence="reads: " + ", ".join(upstream_names[:8]) if upstream_names else f"{df.get('linked_entity_count')} linked entities"))
    if facts["downstream_dataflows"]:
        names = sorted({e.get("dst_name") or e["dst_id"] for e in facts["downstream_dataflows"]})
        hits.append(make_hit("LNK-002", evidence="read by: " + ", ".join(names[:8]), count=len(names)))
    for e in facts["downstream_direct"]:
        if e["attrs"].get("legacy_connector"):
            hits.append(make_hit("LNK-003", e.get("dst_name") or e["dst_id"], f"{e['dst_type']} uses PowerBI.Dataflows"))
    missing = sorted({e["src_id"] for e in facts["upstream_dataflows"] if e["src_id"] not in dfs})
    if missing:
        hits.append(make_hit("LNK-004", evidence="not in the inventory: " + ", ".join(missing[:5]), count=len(missing)))
    for name in finding_names(findings, "computed_entities")[:25]:
        hits.append(make_hit("CMP-001", name, "entity reads another loaded entity"))
    entities = df.get("entity_count") or 0
    if entities > 50:
        hits.append(make_hit("QRY-001", evidence=f"{entities} enabled queries (limit 50)"))
    elif entities >= 40:
        hits.append(make_hit("QRY-002", evidence=f"{entities} enabled queries"))
    for s in sources:
        if s.get("is_misconfigured"):
            hits.append(make_hit("CON-001", source_label(s), "reported as misconfigured"))
    gateways = [s for s in sources if s.get("uses_gateway")]
    if gateways:
        hits.append(make_hit("GW-001", evidence="gateways: " + ", ".join(sorted({s.get("gateway_name") or s.get("gateway_id") or "?" for s in gateways}))))
    if df.get("owner_enabled") is False or not df.get("owner_upn"):
        hits.append(make_hit("OWN-001", evidence=f"owner {df.get('owner_upn') or '(none)'} is " + ("disabled or missing" if df.get("owner_upn") else "not set")))
    name = df.get("dataflow_name") or ""
    if name and not ALLOWED_NAME_CHARS.match(name):
        hits.append(make_hit("NAM-001", evidence=f"name: {name}"))
    if same_name_count > 1:
        hits.append(make_hit("NAM-002", evidence=f"{same_name_count} dataflows named {name!r} in the workspace"))
    if (df.get("schedule_runs_per_day") or 0) > 48:
        hits.append(make_hit("SCH-001", evidence=f"{df['schedule_runs_per_day']} runs per day"))
    if df.get("schedule_enabled") is True:
        hits.append(make_hit("SCH-002", evidence=f"{df.get('schedule_runs_per_day') or '?'} runs per day"))
    if df.get("external_trigger_seen"):
        hits.append(make_hit("TRG-001", evidence="API-triggered refreshes in the observed window"))
    for e in facts["direct_by_type"].get("DataPipeline", []):
        hits.append(make_hit("PIP-001", e.get("dst_name") or e["dst_id"], "RefreshDataFlow activity"))
    if facts["rebind_items"]:
        hits.append(make_hit("REB-001", evidence=f"{len(facts['rebind_items'])} direct downstream items", count=len(facts["rebind_items"])))
    if (df.get("ai_function_count") or 0) > 0:
        hits.append(make_hit("AIX-001", evidence=", ".join(finding_names(findings, "ai_functions")[:5]) or f"{df['ai_function_count']} AI functions"))
    if cfg.require_definition_for_ready and df.get("definition_status") != "Collected":
        hits.append(make_hit("DAT-001", evidence=f"definition status: {df.get('definition_status') or 'not collected'}"))
    if (df.get("consecutive_failures") or 0) >= cfg.failing_consecutive:
        message = truncate(df.get("last_error_message"), 160)
        hits.append(make_hit("REF-001", evidence=f"{df['consecutive_failures']} consecutive failures" + (f": {message}" if message else "")))
    if df.get("refresh_history_status") == "Collected" and facts["total_downstream"] == 0:
        last = df.get("last_success_at") or df.get("modified_at")
        if last is not None and (now - last).days >= cfg.stale_days:
            basis = "last successful refresh" if df.get("last_success_at") else "no successful refresh observed, last modified"
            hits.append(make_hit("STA-001", evidence=f"{basis} {last:%Y-%m-%d}, no downstream consumers"))
    return [h for h in hits if RULES[h["rule_id"]].enabled]


def path_status(hits, path):
    """Worst non-deferred impact for a migration path: Ready / NeedsAttention / Unsupported."""
    key = "impact_wizard" if path == "wizard" else "impact_saveas"
    status = "Ready"
    for h in hits:
        impact = getattr(RULES[h["rule_id"]], key)
        if impact in STATUS_ORDER and STATUS_ORDER[impact] > STATUS_ORDER[status]:
            status = impact
    return status
