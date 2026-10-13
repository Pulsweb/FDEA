# %% [markdown]
# ## 7.2 Complexity score, effort, readiness class and recommendation
# **Complexity score** (0-100) = weighted average of six component scores (each 0-100). Weights are configurable through
# `p_score_weights` (JSON, e.g. `{"gateway": 25, "bands": {"high": 45}}`).
#
# | Component | Default weight | Signals (points are capped at 100) |
# |---|---|---|
# | `dependencies` | 20 | 12 per upstream dataflow, 12 per downstream dataflow, 6 per downstream semantic model, 2 per report, 4 per dashboard, 4 per other asset, +15 crosses workspaces, +25 in a dependency cycle |
# | `refresh_criticality` | 15 | schedule frequency (0-50), p95 duration (0-25), external triggers (+15), business criticality (High +10, Medium +5) |
# | `gateway` | 15 | uses a gateway (50), on-premises source (+25), several gateways (+10 each, max 25) |
# | `source_complexity` | 15 | number of sources (0-55), number of source types (0-30), heaviest category (Fabric 0, database / storage 5, file / SaaS 10, web / other 15) |
# | `unsupported` | 25 | sum of the rule weights hit on the recommended path (x1.25 per additional entity, max x2) |
# | `technical` | 10 | enabled queries (0-50), custom functions (0-15), parameters (0-10), native queries (0-10), script size (0-10), computed + linked entities (0-5) |
#
# Bands (configurable): **Low** < 20, **Medium** < 40, **High** < 60, **Very High** >= 60.
# **Effort** (person-days) = band base (0.5 / 1.5 / 4 / 8) + remediation effort of every rule that needs action on the recommended
# path (1 + 0.5 per additional entity, REB-001 is 0.25 per downstream item to re-point).

# %%
DEFAULT_BANDS = {"very_high": 60.0, "high": 40.0, "medium": 20.0}
PER_ITEM_RULES = {"REB-001"}
POST_MIGRATION_RULES = {"SCH-002", "REB-001", "PIP-001", "LNK-002", "LNK-003", "TRG-001", "INC-001", "AIX-001"}
FIX_FIRST_CATEGORIES = {"Connectivity", "Governance", "Capacity", "Data quality"}
ACTIONABLE = ("Unsupported", "NeedsAttention")
PATH_CODE = {"wizard": "UpgradeWizard", "saveas": "SaveAs"}
PATH_LABEL = {"wizard": "the Upgrade Wizard", "saveas": "Save-As"}
PATH_TEXT = {
    "UpgradeWizard": "upgrade in place with the Upgrade Wizard (keeps the dataflow id and name; the owner runs it for the whole workspace batch)",
    "SaveAs": "create the Gen2 copy with Save-As (new id: re-point the downstream items and re-enable the schedule)",
}


def resolve_scoring(cfg):
    """Component weights and band thresholds from the defaults and p_score_weights."""
    weights, bands = dict(DEFAULT_WEIGHTS), dict(DEFAULT_BANDS)
    for key, value in (cfg.score_weights or {}).items():
        if key == "bands":
            if not isinstance(value, dict):
                raise ConfigError("score_weights.bands must be an object such as {\"high\": 45}")
            for name, limit in value.items():
                if name not in bands:
                    raise ConfigError(f"unknown band {name!r}; expected one of {sorted(bands)}")
                bands[name] = float(limit)
        elif key in weights:
            weights[key] = float(value)
            if weights[key] < 0:
                raise ConfigError(f"score weight {key} must be >= 0")
        else:
            raise ConfigError(f"unknown score weight {key!r}; expected one of {sorted(weights)} or 'bands'")
    if sum(weights.values()) <= 0:
        raise ConfigError("score weights must not all be zero")
    if not bands["medium"] < bands["high"] < bands["very_high"]:
        raise ConfigError("band thresholds must increase: medium < high < very_high")
    return weights, bands


def band_for(score, bands):
    if score >= bands["very_high"]:
        return "Very High"
    if score >= bands["high"]:
        return "High"
    if score >= bands["medium"]:
        return "Medium"
    return "Low"


def path_impact(rule, path):
    return rule.impact_wizard if path == "wizard" else rule.impact_saveas


def hit_groups(hits):
    """Hits grouped per rule: total count, distinct entities and a few evidences."""
    groups = {}
    for h in hits:
        g = groups.setdefault(h["rule_id"], {"rule": RULES[h["rule_id"]], "count": 0, "entities": [], "evidence": []})
        g["count"] += h.get("count", 1)
        if h["entity_name"] and h["entity_name"] not in g["entities"]:
            g["entities"].append(h["entity_name"])
        if h["evidence"] and len(g["evidence"]) < 3:
            g["evidence"].append(h["evidence"])
    return groups


def rule_effort(rule, count):
    if rule.rule_id in PER_ITEM_RULES:
        return rule.effort_days * min(max(count, 1), 40)
    return rule.effort_days * (1 + 0.5 * (min(max(count, 1), 10) - 1))


def remediation_effort(hits, path):
    return sum(rule_effort(g["rule"], g["count"]) for g in hit_groups(hits).values() if path_impact(g["rule"], path) in ACTIONABLE)


# %%
def score_dependencies(facts, in_cycle):
    points = (12 * len(facts["upstream_dataflows"]) + 12 * len(facts["downstream_dataflows"]) + 6 * len(facts["datasets_all"])
              + 2 * len(facts["reports_all"]) + 4 * len(facts["dashboards_all"]) + 4 * len(facts["other_all"])
              + (15 if facts.get("crosses_workspace") else 0) + (25 if in_cycle else 0))
    return float(min(100, points))


def score_refresh(df, criticality):
    runs = df.get("schedule_runs_per_day") or 0
    if df.get("schedule_enabled") is False:
        runs = 0
    duration = df.get("refresh_p95_duration_sec") or df.get("refresh_avg_duration_sec") or df.get("last_refresh_duration_sec")
    points = (bucket(runs, [(1, 15), (4, 30), (12, 40), (24, 50)]) + bucket(duration, [(300, 5), (1800, 15), (3600, 25)])
              + (15 if df.get("external_trigger_seen") else 0) + {"High": 10, "Medium": 5}.get(criticality, 0))
    return float(min(100, points))


def score_gateway(df, sources):
    gateways = {s.get("gateway_id") for s in sources if s.get("gateway_id")}
    points = 50 if (df.get("uses_gateway") or gateways) else 0
    points += 25 if (df.get("uses_on_prem_gateway") or any(s.get("is_on_premises") for s in sources)) else 0
    points += min(25, 10 * max(0, len(gateways) - 1))
    return float(min(100, points))


def score_sources(df, sources):
    count = df.get("source_count") if df.get("source_count") is not None else len(sources)
    types = df.get("source_type_count") if df.get("source_type_count") is not None else len({s.get("datasource_type") for s in sources})
    worst = max((SOURCE_COMPLEXITY.get(s.get("source_category") or "Other", 2.0) for s in sources), default=0.0)
    category = round(15 * (worst - 0.5) / 1.5) if worst else 0
    return float(min(100, bucket(count, [(1, 10), (3, 25), (6, 40), (11, 55)]) + bucket(types, [(2, 10), (4, 20), (7, 30)]) + max(0, category)))


def score_technical(df):
    entities = df.get("entity_count") or df.get("query_count") or 0
    points = (bucket(entities, [(10, 10), (25, 25), (40, 40), (50, 50)]) + bucket(df.get("custom_function_count"), [(1, 5), (4, 10), (10, 15)])
              + bucket(df.get("parameter_count"), [(1, 3), (5, 7), (10, 10)]) + bucket(df.get("native_query_count"), [(1, 5), (5, 10)])
              + bucket(df.get("m_script_chars"), [(20000, 5), (100000, 10)])
              + min(5, (df.get("computed_entity_count") or 0) + (df.get("linked_entity_count") or 0)))
    return float(min(100, points))


def score_unsupported(hits, path):
    """Rule weights of the hits that need action on the path; extra entities of one rule add 25% each (max x2)."""
    counts = Counter()
    for h in hits:
        if path_impact(RULES[h["rule_id"]], path) in ACTIONABLE:
            counts[h["rule_id"]] += h.get("count", 1)
    return float(min(100, sum(RULES[r].weight * (1 + 0.25 * min(n - 1, 4)) for r, n in counts.items())))


def complexity(weights, components, bands):
    score = sum(weights[k] * components[k] for k in weights) / sum(weights.values())
    return round(score, 1), band_for(score, bands)


def estimate_effort(band, hits, path):
    return round(BASE_EFFORT_DAYS[band] + remediation_effort(hits, path), 2)


def runs_per_month(df):
    if df.get("schedule_enabled") is None:
        return None
    if not df.get("schedule_enabled"):
        return 0.0
    days = len(df.get("schedule_days") or []) or 7
    return (df.get("schedule_runs_per_day") or 0) * days * 52 / 12


def estimate_cu(df):
    """Indicative Gen2 standard compute: 12 CU/s for the first 10 minutes of a refresh, 1.5 CU/s afterwards, from Gen1 durations."""
    duration = df.get("refresh_avg_duration_sec") or df.get("last_refresh_duration_sec")
    if not duration:
        return None, None
    per_run = 12.0 * min(duration, 600) + 1.5 * max(0.0, duration - 600)
    runs = runs_per_month(df)
    return round(per_run, 1), (None if runs is None else round(per_run * runs / 3600, 2))


# %%
def choose_path(cfg, statuses, hits):
    """Recommended migration path: the configured preference, else the least severe status, then the least remediation effort."""
    options = ["wizard", "saveas"] if cfg.wizard_available else ["saveas"]
    if cfg.preferred_path in options:
        return cfg.preferred_path
    return min(options, key=lambda p: (STATUS_ORDER[statuses[p]], remediation_effort(hits, p), 0 if p == "wizard" else 1))


def classify_own(cfg, hits, statuses, path, override):
    """(class, reason, recommended path) from the dataflow's own findings. SEQ-001 is applied afterwards by propagation."""
    ids = {h["rule_id"] for h in hits}
    by_id = {h["rule_id"]: h for h in hits}
    if override and override.get("defer"):
        return "Deferred", f"Deferred by governance: {override.get('defer_reason') or 'no reason recorded'}", "None"
    if "STA-001" in ids:
        return "Deferred", f"Stale and unused: {by_id['STA-001']['evidence']}", "Retire"
    status = statuses[path]
    if status == "Unsupported":
        blocking = sorted({h["rule_id"] for h in hits if path_impact(RULES[h["rule_id"]], path) == "Unsupported"})
        fixable = all(RULES[r].category in FIX_FIRST_CATEGORIES for r in blocking)
        return ("Unsupported", f"Blocked on {PATH_LABEL[path]}: " + "; ".join(f"{r} {RULES[r].name}" for r in blocking),
                "FixThenReassess" if fixable else "ManualRebuild")
    if "REF-001" in ids:
        return "Deferred", f"Refresh failing: {by_id['REF-001']['evidence']}", "FixThenReassess"
    if status == "NeedsAttention":
        groups = hit_groups(hits)
        attention = sorted((g for g in groups.values() if path_impact(g["rule"], path) == "NeedsAttention"),
                           key=lambda g: (SEVERITY_ORDER[g["rule"].severity], -g["rule"].weight, g["rule"].rule_id))
        return ("NeedsAttention", f"{len(attention)} finding(s) to resolve on {PATH_LABEL[path]}: " + "; ".join(g["rule"].name for g in attention[:4])
                + (" ..." if len(attention) > 4 else ""), PATH_CODE[path])
    return "Ready", f"No blocking or attention finding on {PATH_LABEL[path]}", PATH_CODE[path]


def build_actions(hits, path, klass):
    """Remediation actions split into before / after the migration, most severe first."""
    wanted = ACTIONABLE + (("Deferred",) if klass == "Deferred" else ())
    groups = [g for g in hit_groups(hits).values() if path_impact(g["rule"], path) in wanted]
    groups.sort(key=lambda g: (0 if path_impact(g["rule"], path) == "Deferred" else 1, SEVERITY_ORDER[g["rule"].severity], -g["rule"].weight, g["rule"].rule_id))
    before, after = [], []
    for g in groups:
        rule = g["rule"]
        text = f"{rule.rule_id}: {rule.remediation}"
        if g["entities"]:
            text += f" [{', '.join(g['entities'][:5])}{' ...' if len(g['entities']) > 5 else ''}]"
        (after if rule.rule_id in POST_MIGRATION_RULES else before).append(text)
    return before, after


def build_recommendation(klass, rec_path, reason, wave_label):
    if klass == "Ready":
        return f"Ready: {PATH_TEXT[rec_path]}. Planned in {wave_label}."
    if klass == "NeedsAttention":
        return f"Needs attention before migrating - {reason}. Then {PATH_TEXT[rec_path]}. Planned in {wave_label}."
    if klass == "Unsupported":
        tail = "Fix the blockers and re-run the assessment." if rec_path == "FixThenReassess" else "Rebuild it as a native Dataflow Gen2 or keep it on Gen1."
        return f"Not migratable as is - {reason}. {tail}"
    if rec_path == "Retire":
        return f"Candidate for retirement - {reason}. Confirm with the owner and decommission instead of migrating."
    if rec_path == "FixThenReassess":
        return f"Deferred - {reason}. Resolve it, then re-run the assessment."
    return f"Deferred - {reason}."
