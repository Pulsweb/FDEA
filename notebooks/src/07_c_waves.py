# %% [markdown]
# ## 7.3 Governance enrichment, dependency order and migration waves
# **Wave logic** (evaluated after the readiness class, score and criticality are known):
#
# | Wave | Rule |
# |---|---|
# | **0 - Pilot** | up to `p_pilot_size` dataflows chosen among the quick wins: isolated (no linked dataflow), **never Prod**, no failing refresh, a verified successful refresh preferred, few consumers, at most a third of the pilot per workspace |
# | **1 - Quick wins** | Ready, Low complexity, business criticality not High |
# | **2 - Standard** | Ready (Medium complexity or High criticality) or Needs Attention (Low / Medium complexity, criticality not High) |
# | **3 - Complex or critical** | Ready with High / Very High complexity, Needs Attention with High / Very High complexity or High criticality |
# | **4 - Remediate or rebuild** | Unsupported |
# | **99 - Backlog** | Deferred (stale, failing refresh, blocked by an upstream, or deferred by governance) |
#
# A **topological constraint** is applied afterwards: among the planned waves (0-4) a dataflow is never planned before the
# dataflows it reads (`wave(d) >= wave(u)`); dependency cycles are condensed and migrate together. The backlog (99) is outside
# the order: a dataflow blocked by an Unsupported or Deferred upstream (rule `SEQ-001`) waits there until the upstream is fixed
# and the assessment re-run. `pin_wave` (0-4) in the governance overrides wins for Ready / Needs Attention dataflows.
# Batches are `wave x workspace`, because the Upgrade Wizard works per workspace.

# %%
SCOPE_IN, SCOPE_OUT, SCOPE_UNKNOWN = "In scope (Gen1 -> Gen2)", "Out of scope (already Gen2)", "Unknown generation"
READINESS_LABEL = {"Ready": "Ready", "NeedsAttention": "Needs Attention", "Unsupported": "Unsupported", "Deferred": "Deferred"}
ENV_RANK = {"Dev": 0, "Test": 1, "Unknown": 2, "Prod": 3}
BLOCKING_CLASSES = ("Unsupported", "Deferred")
PINNABLE_WAVES = (0, 1, 2, 3, 4)
UNKNOWN_WORKSPACE = "unknown"


NOT_AN_ID = {"", "na", "n/a", "n.a.", "n.a", "null", "none", "nan", "nil", "-", "--", "?", "0"}


def index_migrations(rows, known_gen2=()):
    """Save-As accelerator results (table DataflowMigrations) -> {gen1 id: {"ok", "gen2", "copies", "verified"}}.
    A success is never overwritten by a failure; placeholders such as NA are not ids. Save-As is not idempotent, so one Gen1
    dataflow can have several successful copies: the primary one is chosen deterministically (a copy found in the Gen2 inventory
    first, then the smallest id) and all of them are listed. `verified` tells whether a copy exists in the inventory."""
    out = {}
    for raw in rows or []:
        rec = {str(k).lower(): v for k, v in raw.items()}
        gen1, gen2 = gid(rec.get("dfg1_id")), gid(rec.get("dfg2_id"))
        if not gen1:
            continue
        entry = out.setdefault(gen1, {"ok": False, "gen2": None, "copies": set(), "verified": False})
        if str(rec.get("result") or "").strip().lower() in ("succeed", "success", "succeeded") and gen2 and gen2 not in NOT_AN_ID and gen2 != gen1:
            entry["copies"].add(gen2)
    known = set(known_gen2)
    for entry in out.values():
        copies = sorted(entry["copies"], key=lambda c: (c not in known, c))
        entry.update(ok=bool(copies), gen2=copies[0] if copies else None, copies=copies, verified=any(c in known for c in copies))
    return out


def resolve_environment(df, ws, gov, patterns):
    """Override (dataflow, then workspace) > workspace environment (deployment pipeline, naming) > dataflow name > Unknown."""
    for scope, key in (("dataflow", df["dataflow_id"]), ("workspace", df["workspace_id"])):
        declared = (gov[scope].get(key) or {}).get("environment")
        if declared:
            return declared
    ws_env = (ws or {}).get("environment")
    if ws_env and ws_env != "Unknown":
        return ws_env
    return detect_environment(df.get("dataflow_name"), patterns)


def governance_value(field, df, ws, gov):
    """Dataflow level override, else workspace level override, else the workspace row."""
    for scope, key in (("dataflow", df["dataflow_id"]), ("workspace", df["workspace_id"])):
        value = (gov[scope].get(key) or {}).get(field)
        if value not in (None, ""):
            return value
    return (ws or {}).get(field)


def resolve_criticality(df, ws, facts, env, gov):
    declared = governance_value("business_criticality", df, ws, gov)
    if declared:
        return declared, "Override"
    return infer_criticality(df, facts, env), "Inferred"


def assessment_confidence(df):
    """Data completeness of the evidence behind a readiness class."""
    inspected = df.get("definition_status") == "Collected"
    engine = compute_engine_state(df.get("compute_engine_behavior")) != "Unknown"
    refresh = df.get("refresh_history_status") == "Collected"
    if inspected and engine and refresh:
        return "High"
    return "Low" if not inspected and not engine else "Medium"


def _name(a):
    return a["df"].get("dataflow_name") or a["df"]["dataflow_id"]


def sequence_graph(A, edges):
    """Dataflow -> dataflow adjacency among the dataflows being planned."""
    succ, preds = defaultdict(set), defaultdict(set)
    for e in edges:
        if e["src_type"] == "Dataflow" and e["dst_type"] == "Dataflow" and e["src_id"] in A and e["dst_id"] in A and e["src_id"] != e["dst_id"]:
            succ[e["src_id"]].add(e["dst_id"])
            preds[e["dst_id"]].add(e["src_id"])
    return succ, preds


def propagate_sequence(A, preds, comps):
    """Rule SEQ-001: upstream dataflows migrate first, so a dataflow reading an Unsupported or Deferred one is Deferred."""
    comp_of = {m: i for i, comp in enumerate(comps) for m in comp}
    for i, comp in enumerate(comps):
        blockers = {p: A[p]["klass"] for m in comp for p in preds.get(m, ()) if comp_of[p] != i and A[p]["klass"] in BLOCKING_CLASSES}
        if len(comp) > 1:
            blockers.update({m: A[m]["klass"] for m in comp if A[m]["klass"] in BLOCKING_CLASSES})
        for m in comp:
            others = {p: k for p, k in blockers.items() if p != m}
            if others and A[m]["klass"] not in BLOCKING_CLASSES:
                names = ", ".join(f"{_name(A[p])} ({k})" for p, k in sorted(others.items())[:4])
                A[m]["hits"].append(make_hit("SEQ-001", evidence=f"blocked by {names}", count=len(others)))
                A[m]["klass"], A[m]["reason"], A[m]["rec_path"] = "Deferred", f"Blocked by upstream: {names}", "FixThenReassess"


def dependency_levels(comps, preds):
    comp_of = {m: i for i, comp in enumerate(comps) for m in comp}
    level = {}
    for i, comp in enumerate(comps):
        ups = {comp_of[p] for m in comp for p in preds.get(m, ()) if comp_of[p] != i}
        level[i] = 1 + max((level[u] for u in ups), default=-1)
    return {m: level[i] for i, comp in enumerate(comps) for m in comp}


def base_wave(a):
    klass, band, crit = a["klass"], a["band"], a["crit"]
    if klass == "Deferred":
        return 99, f"Deferred: {a['reason']}"
    if klass == "Unsupported":
        return 4, f"Remediate or rebuild: {a['reason']}"
    heavy = band in ("High", "Very High")
    if klass == "Ready":
        if heavy:
            return 3, f"Ready but {band} complexity"
        if band == "Low" and crit != "High":
            return 1, "Ready, low complexity and not business critical: quick win"
        return 2, "Ready, business critical" if crit == "High" else f"Ready, {band} complexity"
    if heavy or crit == "High":
        return 3, f"Needs attention, {band} complexity" + (", High criticality" if crit == "High" else "")
    return 2, f"Needs attention, {band} complexity"


def pick_pilot(cfg, A, succ, preds):
    """Representative low-risk quick wins: isolated, never Prod, a verified healthy refresh baseline first (then the unverified ones to
    fill the pilot), few consumers, spread over workspaces (at most a third of the pilot per workspace)."""
    if cfg.pilot_size <= 0:
        return []
    cap = max(1, cfg.pilot_size // 3)
    candidates = []
    for did, a in A.items():
        df = a["df"]
        failing = df.get("last_refresh_status") == "Failed" or bool(df.get("consecutive_failures"))
        if a["base_wave"] == 1 and not succ.get(did) and not preds.get(did) and a["env"] != "Prod" and not failing:
            consumers = len(a["facts"]["datasets_all"]) + len(a["facts"]["reports_all"]) + len(a["facts"]["dashboards_all"])
            verified = df.get("last_success_at") is not None
            candidates.append(((0 if verified else 1, ENV_RANK.get(a["env"], 2), 0 if 1 <= consumers <= 5 else 1, a["score"], a["ws_name"] or "", _name(a), did), did))
    chosen, per_workspace = [], Counter()
    for _, did in sorted(candidates):
        wid = A[did]["df"]["workspace_id"]
        if len(chosen) < cfg.pilot_size and per_workspace[wid] < cap:
            chosen.append(did)
            per_workspace[wid] += 1
    return chosen


def plan_waves(cfg, A, succ, preds, comps, pins):
    """Assign a wave to every planned dataflow. Planned waves (0-4) respect the dependency order; Deferred (99) is the backlog,
    outside the order (a blocked dataflow waits there until its upstream is fixed and the assessment re-run)."""
    for a in A.values():
        a["base_wave"], a["wave_reason"] = base_wave(a)
    for did in pick_pilot(cfg, A, succ, preds):
        a = A[did]
        a["base_wave"] = 0
        baseline = "" if a["df"].get("last_success_at") is not None else " (refresh baseline not verified)"
        a["wave_reason"] = (f"Pilot: low-risk {a['env']} dataflow with {len(a['facts']['datasets_all']) + len(a['facts']['reports_all'])} "
                            f"downstream model(s) / report(s){baseline}")
    comp_of = {m: i for i, comp in enumerate(comps) for m in comp}
    for i, comp in enumerate(comps):
        floor, pusher = -1, None
        for m in comp:
            for p in sorted(preds.get(m, ())):
                if comp_of[p] != i and A[p]["wave"] != 99 and A[p]["wave"] > floor:
                    floor, pusher = A[p]["wave"], p
        movable = [m for m in comp if A[m]["klass"] not in BLOCKING_CLASSES]
        group_wave = max([A[m]["base_wave"] for m in movable] + [floor]) if movable else None
        for m in comp:
            a = A[m]
            if a["klass"] in BLOCKING_CLASSES:
                a["wave"] = a["base_wave"]
                continue
            wave, reason = group_wave, a["wave_reason"]
            if wave > a["base_wave"]:
                reason += (f"; moved from wave {a['base_wave']} to {wave} because upstream '{_name(A[pusher])}' is planned in wave {floor}"
                           if floor >= max(A[x]["base_wave"] for x in movable) else f"; moved from wave {a['base_wave']} to {wave} to migrate linked dataflows together")
            pin = pins.get(m)
            if pin in PINNABLE_WAVES:
                reason = f"Pinned to wave {pin} by governance" + (f" (upstream '{_name(A[pusher])}' is planned later, in wave {floor})" if pin < floor else "")
                wave = pin
            a["wave"], a["wave_reason"] = wave, reason
    for a in A.values():
        a["wave_label"] = WAVE_LABELS.get(a["wave"], f"Wave {a['wave']}")
        a["batch_id"] = f"W{a['wave']:02d}-{a['df']['workspace_id']}"
