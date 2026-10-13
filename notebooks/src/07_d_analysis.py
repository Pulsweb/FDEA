# %% [markdown]
# ## 7.4 Analysis engine
# `run_analysis` turns the Silver rows into every Gold table. It is deterministic and side-effect free (inputs in, rows out),
# so it is unit-tested without Spark and can be re-run on its own (`p_run_mode = "analysis_only"`).

# %%
FEATURE_FAMILIES = {"CAP": "Capacity", "STO": "AttachedStorage", "ECE": "EnhancedCompute", "DQ": "DirectQuery", "INC": "IncrementalRefresh",
                    "LNK": "LinkedEntities", "CMP": "ComputedEntities", "QRY": "QueryLimit", "CON": "Connections", "GW": "Gateway",
                    "OWN": "Ownership", "NAM": "Naming", "SCH": "Schedule", "TRG": "ExternalTriggers", "PIP": "Pipelines", "REB": "Rebind",
                    "AIX": "AIInsights", "REF": "RefreshFailures", "STA": "Stale", "SEQ": "UpstreamBlocked", "DAT": "DefinitionNotInspected"}
REBIND_ACTIONS = {
    "Dataflow": "Open the downstream dataflow in the editor and save it so that its linked entities bind to the migrated dataflow",
    "SemanticModel": "Re-point the semantic model source to the migrated dataflow (connector PowerPlatform.Dataflows), take over if needed, then refresh",
    "Datamart": "Re-point the datamart source to the migrated dataflow, then refresh",
    "DataPipeline": "Update the Dataflow refresh activity to the id of the migrated dataflow",
}


def dataflow_cycles(edges):
    """Ids of dataflows that are part of a dependency cycle (a dataflow that reads itself included)."""
    succ, ids, reads_itself = defaultdict(set), set(), set()
    for e in edges:
        if e.get("src_type") == "Dataflow" and e.get("dst_type") == "Dataflow":
            if e["src_id"] == e["dst_id"]:
                reads_itself.add(e["src_id"])
            else:
                succ[e["src_id"]].add(e["dst_id"])
                ids.update((e["src_id"], e["dst_id"]))
    return {m for comp in strongly_connected_components(sorted(ids), succ) if len(comp) > 1 for m in comp} | reads_itself


def migration_state(df, migrations, gen2_targets):
    """(scope, migration_status, gen2_dataflow_id) of a dataflow."""
    did, kind = df["dataflow_id"], df.get("dataflow_type")
    if kind == "Gen2":
        if did in gen2_targets:
            return SCOPE_OUT, "SaveAsCopy", None
        return SCOPE_OUT, "Migrated" if df.get("first_seen_dataflow_type") == "Gen1" else None, None
    if kind != "Gen1":
        return SCOPE_UNKNOWN, None, None
    attempt = migrations.get(did)
    if attempt and attempt["ok"]:
        return SCOPE_IN, "MigratedBySaveAs", attempt["gen2"]
    return SCOPE_IN, "SaveAsFailed" if attempt else "NotStarted", None


def assess_dataflows(cfg, now, dfs, wss, sources, edges, graph, gov, migrations, cycles):
    """Rules, readiness class, score, effort, recommendation and wave for every Gen1 dataflow that is still to be migrated."""
    weights, bands = resolve_scoring(cfg)
    patterns = {**DEFAULT_ENV_PATTERNS, **(cfg.environment_patterns or {})}
    names = Counter((r["workspace_id"], (r.get("dataflow_name") or "").strip().lower()) for r in dfs.values() if r.get("dataflow_type") == "Gen1")
    A = {}
    for did, df in dfs.items():
        if df.get("dataflow_type") != "Gen1" or migrations.get(did, {}).get("ok"):
            continue
        ws = wss.get(df["workspace_id"])
        facts = direct_facts(df, graph, dfs)
        hits = evaluate_rules(cfg, now, df, ws, sources.get(did, []), facts, graph, dfs, names[(df["workspace_id"], (df.get("dataflow_name") or "").strip().lower())])
        env = resolve_environment(df, ws, gov, patterns)
        crit, crit_source = resolve_criticality(df, ws, facts, env, gov)
        A[did] = {"df": df, "ws": ws, "ws_name": (ws or {}).get("workspace_name"), "facts": facts, "hits": hits, "env": env, "crit": crit, "crit_source": crit_source}
    succ, preds = sequence_graph(A, edges)
    comps = strongly_connected_components(sorted(A), succ)
    for did, a in A.items():
        statuses = {"wizard": path_status(a["hits"], "wizard"), "saveas": path_status(a["hits"], "saveas")}
        a["status"], a["path"] = statuses, choose_path(cfg, statuses, a["hits"])
        a["klass"], a["reason"], a["rec_path"] = classify_own(cfg, a["hits"], statuses, a["path"], gov["dataflow"].get(did))
    propagate_sequence(A, preds, comps)
    for did, a in A.items():
        listed = sources.get(did, [])
        a["components"] = {"dependencies": score_dependencies(a["facts"], did in cycles), "refresh_criticality": score_refresh(a["df"], a["crit"]),
                           "gateway": score_gateway(a["df"], listed), "source_complexity": score_sources(a["df"], listed),
                           "unsupported": score_unsupported(a["hits"], a["path"]), "technical": score_technical(a["df"])}
        a["score"], a["band"] = complexity(weights, a["components"], bands)
        a["effort"] = 0.25 if a["rec_path"] == "Retire" else estimate_effort(a["band"], a["hits"], a["path"])
        a["actions"] = build_actions(a["hits"], a["path"], a["klass"])
    pins = {}
    for did, a in A.items():
        pin = coerce_value("INT", governance_value("pin_wave", a["df"], a["ws"], gov))
        if pin is not None:
            pins[did] = pin
    plan_waves(cfg, A, succ, preds, comps, pins)
    levels = dependency_levels(comps, preds)
    groups = {}
    for members in weak_components(sorted(A), succ):
        if len(members) > 1:
            gid_ = "G" + short_hash("|".join(sorted(members)), 10)
            groups.update({m: (gid_, len(members)) for m in members})
    for did, a in A.items():
        a["level"], (a["group_id"], a["group_size"]) = levels.get(did, 0), groups.get(did, (None, 1))
        a["recommendation"] = build_recommendation(a["klass"], a["rec_path"], a["reason"], a["wave_label"])
    return A, succ, preds


# %%
def unsupported_rows(a, now):
    """One row per (rule, entity). Hits that share both are merged (evidence joined, effort added) instead of overwriting each other."""
    rows = {}
    df = a["df"]
    for h in a["hits"]:
        rule = RULES[h["rule_id"]]
        effort = rule.effort_days * (h.get("count", 1) if rule.rule_id in PER_ITEM_RULES else 1)
        key = (rule.rule_id, h["entity_name"])
        if key in rows:
            row = rows[key]
            row["effort_days"] = round(row["effort_days"] + effort, 2)
            if h["evidence"] and h["evidence"] not in row["evidence"]:
                row["evidence"] = truncate(f"{row['evidence']}; {h['evidence']}" if row["evidence"] else h["evidence"], 1000)
            continue
        rows[key] = {
            "dataflow_id": df["dataflow_id"], "workspace_id": df["workspace_id"], "rule_id": rule.rule_id,
            "feature_code": FEATURE_FAMILIES.get(rule.rule_id.split("-")[0], "Other"), "feature_name": rule.name, "category": rule.category,
            "severity": rule.severity, "impact_wizard": rule.impact_wizard, "impact_saveas": rule.impact_saveas, "entity_name": h["entity_name"],
            "evidence": h["evidence"], "remediation": rule.remediation, "effort_days": round(effort, 2), "weight": rule.weight,
            "confidence": rule.confidence, "detected_at": now}
    return list(rows.values())


def readiness_row(now, a, migration):
    df, facts, comps = a["df"], a["facts"], a["components"]
    blocking = sorted({h["rule_id"] for h in a["hits"] if path_impact(RULES[h["rule_id"]], a["path"]) == "Unsupported"})
    attention = sorted({h["rule_id"] for h in a["hits"] if path_impact(RULES[h["rule_id"]], a["path"]) == "NeedsAttention"})
    cu_run, cu_month = estimate_cu(df)
    before, after = a["actions"]
    return {
        "dataflow_id": df["dataflow_id"], "workspace_id": df["workspace_id"], "dataflow_type": "Gen1", "migration_scope": SCOPE_IN,
        "migration_status": migration[0], "gen2_dataflow_id": migration[1], "readiness": READINESS_LABEL[a["klass"]], "readiness_reason": a["reason"],
        "status_wizard": STATUS_LABEL[a["status"]["wizard"]], "status_saveas": STATUS_LABEL[a["status"]["saveas"]],
        "predicted_wizard_status": WIZARD_NAME[a["status"]["wizard"]], "recommended_path": a["rec_path"], "recommendation": a["recommendation"],
        "top_actions": (before + after)[:6], "blocking_rules": blocking, "attention_rules": attention, "complexity_score": a["score"],
        "complexity_band": a["band"], "score_dependencies": comps["dependencies"], "score_refresh_criticality": comps["refresh_criticality"],
        "score_gateway": comps["gateway"], "score_source_complexity": comps["source_complexity"], "score_unsupported": comps["unsupported"],
        "score_technical": comps["technical"], "effort_days": a["effort"], "est_gen2_cu_seconds_per_run": cu_run, "est_gen2_cu_hours_per_month": cu_month,
        "wave": a["wave"], "wave_label": a["wave_label"], "wave_rationale": a["wave_reason"], "batch_id": a["batch_id"],
        "migration_group_id": a["group_id"], "migration_group_size": a["group_size"], "dependency_level": a["level"],
        "upstream_dataflow_count": len(facts["upstream_dataflows"]), "downstream_dataflow_count": len(facts["downstream_dataflows"]),
        "downstream_dataset_count": len(facts["datasets_all"]), "downstream_report_count": len(facts["reports_all"]),
        "downstream_dashboard_count": len(facts["dashboards_all"]), "downstream_other_count": len(facts["other_all"]),
        "rebind_item_count": len(facts["rebind_items"]), "business_criticality": a["crit"], "criticality_source": a["crit_source"],
        "environment": a["env"], "assessment_confidence": assessment_confidence(df), "rules_version": RULES_VERSION, "assessed_at": now}


def passive_row(now, df, facts, scope, migration, env, crit, entry=None):
    """Readiness row for dataflows that are not planned: already Gen2, unknown generation or already copied with Save-As."""
    if migration[0] == "MigratedBySaveAs":
        copies = (entry or {}).get("copies") or []
        note = f"Copied with Save-As to {migration[1]}: re-point the downstream items (Assessment_RebindWorklist), validate, then retire this Gen1 dataflow."
        if len(copies) > 1:
            note += f" {len(copies)} Gen2 copies exist ({', '.join(copies[:4])}): keep one and delete the others."
        if entry and not entry.get("verified"):
            note += " No Gen2 copy was found in the inventory (deleted, or not scanned yet)."
    elif migration[0] == "SaveAsCopy":
        note = "Gen2 copy created with Save-As: validate it, re-enable its schedule and re-point the downstream items."
    elif migration[0] == "Migrated":
        note = "Already Gen2 (migrated)."
    elif scope == SCOPE_UNKNOWN:
        note = "Generation could not be determined: check access to the workspace and re-run discovery."
    else:
        note = "Native Dataflow Gen2: out of scope."
    return {"dataflow_id": df["dataflow_id"], "workspace_id": df["workspace_id"], "dataflow_type": df.get("dataflow_type") or "Unknown",
            "migration_scope": scope, "migration_status": migration[0], "gen2_dataflow_id": migration[1], "recommended_path": "None",
            "recommendation": note, "upstream_dataflow_count": len(facts["upstream_dataflows"]), "downstream_dataflow_count": len(facts["downstream_dataflows"]),
            "downstream_dataset_count": len(facts["datasets_all"]), "downstream_report_count": len(facts["reports_all"]),
            "downstream_dashboard_count": len(facts["dashboards_all"]), "downstream_other_count": len(facts["other_all"]),
            "rebind_item_count": len(facts["rebind_items"]), "business_criticality": crit, "environment": env, "rules_version": RULES_VERSION, "assessed_at": now}


def summary_row(df, ws, ready, unsupported_count, gov):
    ws = ws or {}
    ok = None if df.get("last_refresh_status") is None else (df["last_refresh_status"] == "Success" and not (df.get("consecutive_failures") or 0))
    keep = ("upstream_dataflow_count", "downstream_dataflow_count", "downstream_dataset_count", "downstream_report_count", "downstream_dashboard_count",
            "migration_scope", "readiness", "predicted_wizard_status", "complexity_score", "complexity_band", "effort_days", "recommended_path",
            "wave", "wave_label", "batch_id", "migration_status", "assessment_confidence", "business_criticality")
    row = {
        "dataflow_id": df["dataflow_id"], "workspace_id": df["workspace_id"], "workspace_name": ws.get("workspace_name"), "capacity_id": ws.get("capacity_id"),
        "capacity_name": ws.get("capacity_name"), "capacity_sku": ws.get("capacity_sku"), "domain_name": ws.get("domain_name"),
        "environment": ready.get("environment") or ws.get("environment"), "workspace_classification": ws.get("workspace_classification"),
        "support_team": governance_value("support_team", df, ws, gov), "business_owner": governance_value("business_owner", df, ws, gov),
        "dataflow_name": df.get("dataflow_name"), "dataflow_type": df.get("dataflow_type"),
        "owner_upn": (gov["dataflow"].get(df["dataflow_id"]) or {}).get("technical_owner") or df.get("owner_upn"), "created_at": df.get("created_at"),
        "modified_at": df.get("modified_at"), "description": df.get("description"), "tags": df.get("tags"), "last_refresh_at": df.get("last_refresh_at"),
        "last_refresh_status": df.get("last_refresh_status"), "last_refresh_duration_sec": df.get("last_refresh_duration_sec"),
        "refresh_success_rate": df.get("refresh_success_rate"), "refresh_ok": ok, "schedule_enabled": df.get("schedule_enabled"),
        "schedule_runs_per_day": df.get("schedule_runs_per_day"), "source_count": df.get("source_count"), "uses_gateway": df.get("uses_gateway"),
        "query_count": df.get("query_count"), "entity_count": df.get("entity_count"), "custom_function_count": df.get("custom_function_count"),
        "parameter_count": df.get("parameter_count"), "incremental_refresh_count": df.get("incremental_refresh_count"),
        "computed_entity_count": df.get("computed_entity_count"), "linked_entity_count": df.get("linked_entity_count"),
        "unsupported_feature_count": unsupported_count, "wizard_status_actual": governance_value("wizard_status_actual", df, ws, gov)}
    row.update({k: ready.get(k) for k in keep})
    return row


# %%
MAX_DEPENDENCY_TOTAL = 500000


def dependency_rows(graph, dfs, wss, cycles):
    """Closure rows per dataflow and direction: at most MAX_DEPENDENCY_ROWS each (nearest first) and MAX_DEPENDENCY_TOTAL overall; once
    the overall budget is spent only the direct (hop 1) relations are listed. Returns (rows, number of closures that may be incomplete)."""
    rows, truncated = [], 0
    for did in sorted(d for d in dfs if d in graph.nodes):
        df = dfs[did]
        anchor = _label(did, graph, dfs)
        for direction in ("UPSTREAM", "DOWNSTREAM"):
            arrow = " <- " if direction == "UPSTREAM" else " -> "
            if len(rows) >= MAX_DEPENDENCY_TOTAL:
                reached, _ = graph.closure_info(did, direction, max_depth=1, limit=MAX_DEPENDENCY_ROWS)
                cut = True
            else:
                reached, cut = graph.closure_info(did, direction, limit=min(MAX_DEPENDENCY_ROWS, MAX_DEPENDENCY_TOTAL - len(rows)))
            truncated += bool(cut)
            for nid, hop, edge, path in reached:
                node = graph.nodes.get(nid, {})
                rel_ws = node.get("workspace_id")
                rows.append({
                    "dataflow_id": did, "workspace_id": df["workspace_id"], "direction": direction, "hop": hop,
                    "related_asset_type": node.get("type") or "Other", "related_asset_id": nid, "related_asset_name": _label(nid, graph, dfs),
                    "related_workspace_id": rel_ws, "related_workspace_name": (wss.get(rel_ws) or {}).get("workspace_name"),
                    "relationship": edge["relationship"], "crosses_workspace": bool(rel_ws and rel_ws != df["workspace_id"]),
                    "related_generation": (dfs.get(nid) or {}).get("generation"),
                    "path": truncate(arrow.join([anchor] + [_label(p, graph, dfs) for p in path[1:]]), 1000), "is_cyclic": did in cycles})
    return rows, truncated


def node_rows(graph, dfs, wss, ready_by_id):
    rows = []
    for nid in sorted(set(graph.nodes) | set(dfs)):
        df, node = dfs.get(nid), graph.nodes.get(nid, {})
        ws_id = (df or {}).get("workspace_id") or node.get("workspace_id")
        ready = ready_by_id.get(nid) or {}
        rows.append({
            "node_id": nid, "node_type": "Dataflow" if df else (node.get("type") or "Other"), "node_name": _label(nid, graph, dfs), "workspace_id": ws_id,
            "workspace_name": (wss.get(ws_id) or {}).get("workspace_name"), "generation": (df or {}).get("generation") if df else None,
            "readiness": ready.get("readiness"), "wave": ready.get("wave"), "in_degree": len({e["src_id"] for e in graph.up.get(nid, [])}),
            "out_degree": len({e["dst_id"] for e in graph.down.get(nid, [])})})
    return rows


def batch_environment(members):
    """Environment of a batch: the one its dataflows share (Unknown ignored when another is known), Mixed when they disagree."""
    envs = {m["env"] for m in members} - {"Unknown"}
    if not envs:
        return "Unknown"
    return next(iter(envs)) if len(envs) == 1 else "Mixed"


def wave_plan_rows(A, wss):
    batches = defaultdict(list)
    for a in A.values():
        batches[(a["wave"], a["df"]["workspace_id"])].append(a)
    rows = []
    for (wave, wid), members in sorted(batches.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        ws = wss.get(wid) or {}
        consumers = set()
        for m in members:
            consumers |= m["facts"]["datasets_all"] | m["facts"]["reports_all"] | m["facts"]["dashboards_all"]
        count = Counter(m["klass"] for m in members)
        rows.append({
            "wave": wave, "wave_label": WAVE_LABELS.get(wave, f"Wave {wave}"), "workspace_id": wid, "workspace_name": ws.get("workspace_name"),
            "capacity_name": ws.get("capacity_name"), "environment": batch_environment(members), "dataflow_count": len(members),
            "ready_count": count["Ready"], "needs_attention_count": count["NeedsAttention"], "unsupported_count": count["Unsupported"],
            "deferred_count": count["Deferred"], "total_effort_days": round(sum(m["effort"] for m in members), 2),
            "max_complexity_score": max(m["score"] for m in members), "downstream_item_count": len(consumers),
            "path_mix": ", ".join(f"{p}:{n}" for p, n in sorted(Counter(m["rec_path"] for m in members).items()))})
    return rows


def backlog_rows(A, preds, migrations):
    ordered = sorted(A.values(), key=lambda a: (a["wave"], a["level"], a["ws_name"] or "", _name(a), a["df"]["dataflow_id"]))
    rows = []
    for seq, a in enumerate(ordered, start=1):
        did = a["df"]["dataflow_id"]
        blocked = sorted(preds.get(did, ()))
        before, after = a["actions"]
        rows.append({
            "seq": seq, "wave": a["wave"], "wave_label": a["wave_label"], "batch_id": a["batch_id"], "workspace_id": a["df"]["workspace_id"],
            "workspace_name": a["ws_name"], "dataflow_id": did, "dataflow_name": a["df"].get("dataflow_name"), "owner_upn": a["df"].get("owner_upn"),
            "readiness": READINESS_LABEL[a["klass"]], "recommended_path": a["rec_path"], "can_start_now": a["klass"] == "Ready" and not blocked,
            "blocked_by": blocked, "pre_migration_actions": before, "post_migration_actions": after, "effort_days": a["effort"],
            "complexity_band": a["band"], "business_criticality": a["crit"], "environment": a["env"],
            "migration_status": "SaveAsFailed" if did in migrations else "NotStarted", "gen2_dataflow_id": None})
    return rows


def rebind_rows(A, dfs, graph, wss, migrations):
    """Consumers to re-point: planned dataflows on their recommended path, plus Gen1 dataflows already copied with Save-As."""
    targets = [(did, a["df"], a["facts"], a["wave"], a["rec_path"], a["ws_name"]) for did, a in A.items()
               if a["klass"] in ("Ready", "NeedsAttention") and a["rec_path"] in ("UpgradeWizard", "SaveAs")]
    for did, attempt in sorted(migrations.items()):
        df = dfs.get(did)
        if attempt["ok"] and df and df.get("dataflow_type") == "Gen1":
            targets.append((did, df, direct_facts(df, graph, dfs), None, "SaveAs", (wss.get(df["workspace_id"]) or {}).get("workspace_name")))
    rows = []
    for did, df, facts, wave, path, ws_name in targets:
        for e in facts["rebind_items"]:
            if path == "UpgradeWizard" and e["dst_type"] != "Dataflow":
                continue
            rows.append({
                "dataflow_id": did, "dataflow_name": df.get("dataflow_name"), "workspace_id": df["workspace_id"], "workspace_name": ws_name, "wave": wave,
                "path": path, "consumer_type": e["dst_type"], "consumer_id": e["dst_id"], "consumer_name": e.get("dst_name"),
                "consumer_workspace_id": e.get("dst_workspace_id"), "consumer_workspace_name": (wss.get(e.get("dst_workspace_id")) or {}).get("workspace_name"),
                "uses_legacy_connector": bool(e["attrs"].get("legacy_connector")), "storage_mode": e["attrs"].get("storage_mode"),
                "action": REBIND_ACTIONS[e["dst_type"]], "effort_days": RULES["REB-001"].effort_days, "gen2_dataflow_id": (migrations.get(did) or {}).get("gen2")})
    return rows


# %%
def conform(table_name, rows):
    """Full column set per row (missing columns become NULL). Unknown keys are a programming error: they would be dropped silently."""
    table = TABLES[table_name]
    known = {c[0] for c in table.cols}
    out = []
    for row in rows:
        stray = set(row) - known
        if stray:
            raise ValueError(f"{table_name}: unknown columns {sorted(stray)}")
        out.append({c: row.get(c) for c in known})
    return out


def run_analysis(cfg, now, data):
    """Silver rows in, Gold rows out. data: dataflows, workspaces, sources, edges, overrides, migrations (lists of dict rows).
    Defensive on purpose: rows without a key are skipped, a dataflow without a workspace is planned under UNKNOWN_WORKSPACE, edges without
    both endpoints are ignored (counted in the stats), and every input is sorted by its key so that row order never changes a result."""
    raw_dataflows = [r for r in data["dataflows"] if r.get("dataflow_id")]
    dfs = {}
    for r in sorted(raw_dataflows, key=lambda r: r["dataflow_id"]):
        dfs[r["dataflow_id"]] = r if r.get("workspace_id") else {**r, "workspace_id": UNKNOWN_WORKSPACE}
    wss = {r["workspace_id"]: r for r in sorted((r for r in data["workspaces"] if r.get("workspace_id")), key=lambda r: r["workspace_id"])}
    sources = defaultdict(list)
    for s in sorted((s for s in data["sources"] if s.get("dataflow_id")), key=lambda s: (s["dataflow_id"], str(s.get("source_key")))):
        sources[s["dataflow_id"]].append(s)
    edges = dedupe_edges(data["edges"])
    incomplete_edges = sum(1 for e in data["edges"] if not (e.get("src_id") and e.get("dst_id")))
    graph = LineageGraph(edges)
    gov = governance_index(data["overrides"])
    known_gen2 = {did for did, r in dfs.items() if r.get("dataflow_type") == "Gen2"}
    migrations = index_migrations(data.get("migrations"), known_gen2)
    gen2_targets = {c for m in migrations.values() for c in m["copies"]}
    cycles = dataflow_cycles(edges)
    A, succ, preds = assess_dataflows(cfg, now, dfs, wss, sources, edges, graph, gov, migrations, cycles)
    patterns = {**DEFAULT_ENV_PATTERNS, **(cfg.environment_patterns or {})}

    readiness, unsupported = [], []
    for did, df in dfs.items():
        scope, status, gen2_id = migration_state(df, migrations, gen2_targets)
        if did in A:
            readiness.append(readiness_row(now, A[did], (status, gen2_id)))
            unsupported.extend(unsupported_rows(A[did], now))
        else:
            facts = direct_facts(df, graph, dfs)
            ws = wss.get(df["workspace_id"])
            env = resolve_environment(df, ws, gov, patterns)
            readiness.append(passive_row(now, df, facts, scope, (status, gen2_id), env, governance_value("business_criticality", df, ws, gov), migrations.get(did)))
    ready_by_id = {r["dataflow_id"]: r for r in readiness}
    feature_counts = Counter(r["dataflow_id"] for r in unsupported if "NeedsAttention" in (r["impact_wizard"], r["impact_saveas"]) or "Unsupported" in (r["impact_wizard"], r["impact_saveas"]))
    summary = [summary_row(df, wss.get(df["workspace_id"]), ready_by_id[did], feature_counts[did], gov) for did, df in dfs.items()]
    dependencies, truncated = dependency_rows(graph, dfs, wss, cycles)
    tables = {
        "Assessment_MigrationReadiness": readiness, "Assessment_UnsupportedFeatures": unsupported, "Assessment_DataflowSummary": summary,
        "Assessment_DataflowDependencies": dependencies, "Assessment_LineageNodes": node_rows(graph, dfs, wss, ready_by_id),
        "Assessment_WavePlan": wave_plan_rows(A, wss), "Assessment_MigrationBacklog": backlog_rows(A, preds, {k: v for k, v in migrations.items() if not v["ok"]}),
        "Assessment_RebindWorklist": rebind_rows(A, dfs, graph, wss, migrations), "Assessment_RuleCatalog": rule_catalog_rows()}
    tables = {name: conform(name, rows) for name, rows in tables.items()}
    classes = Counter(r["readiness"] for r in readiness if r.get("readiness"))
    stats = {"dataflows": len(dfs), "planned": len(A), "classes": dict(classes), "cycles": len(cycles), "dependency_truncated": truncated,
             "incomplete_edges": incomplete_edges, "dataflows_without_workspace": sum(1 for r in raw_dataflows if not r.get("workspace_id")),
             "migrated": sum(1 for r in readiness if r.get("migration_status") in ("Migrated", "MigratedBySaveAs")),
             "effort_days": round(sum(a["effort"] for a in A.values()), 2)}
    return {"tables": tables, "stats": stats}


GOLD_TABLES = ("Assessment_MigrationReadiness", "Assessment_UnsupportedFeatures", "Assessment_DataflowSummary", "Assessment_DataflowDependencies",
               "Assessment_LineageNodes", "Assessment_WavePlan", "Assessment_MigrationBacklog", "Assessment_RebindWorklist", "Assessment_RuleCatalog")


def load_analysis_inputs(ctx):
    lake = ctx.lake
    return {"dataflows": lake.read("Assessment_Dataflows"), "workspaces": lake.read("Assessment_Workspaces"),
            "sources": lake.read("Assessment_DataflowSources"), "edges": lake.read("Assessment_Lineage"),
            "overrides": lake.read("Assessment_GovernanceOverrides"), "migrations": lake.read_external(ctx.cfg.migration_results_table)}


def phase_analysis(ctx):
    data = load_analysis_inputs(ctx)
    if data["migrations"] is None:
        ctx.logger.info("no Save-As results table %r: migration status derives from type changes only", ctx.cfg.migration_results_table)
    result = run_analysis(ctx.cfg, ctx.clock.now(), data)
    ctx.state["analysis"] = result
    stats = result["stats"]
    if stats["dependency_truncated"]:
        ctx.logger.warning("%d dependency closures may be incomplete (at most %d rows per dataflow and direction, %d overall, nearest assets first; "
                           "the counts in Assessment_MigrationReadiness stay exact)", stats["dependency_truncated"], MAX_DEPENDENCY_ROWS, MAX_DEPENDENCY_TOTAL)
    if stats["incomplete_edges"]:
        ctx.logger.warning("%d lineage rows without a source or a target id were ignored", stats["incomplete_edges"])
    if stats["dataflows_without_workspace"]:
        ctx.logger.warning("%d dataflows have no workspace id: they are planned under workspace %r", stats["dataflows_without_workspace"], UNKNOWN_WORKSPACE)
    classes = ", ".join(f"{k}={v}" for k, v in sorted(stats["classes"].items())) or "no in-scope dataflow"
    return {"rows_in": len(data["dataflows"]), "rows_out": sum(len(v) for v in result["tables"].values()),
            "message": f"{stats['planned']} planned of {stats['dataflows']} dataflows: {classes}; effort {stats['effort_days']} d"}

# %% [markdown]
# ### Run - section 7: migration readiness analysis

# %% tags=["run"]
run_steps(ctx, sections={7})
