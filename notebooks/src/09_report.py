# %% [markdown]
# # 9. Reporting output
# Everything is derived from the Gold / Silver tables, so it also works after `p_run_mode = "analysis_only"`.
#
# | Output | Where | Purpose |
# |---|---|---|
# | KPI summary (Markdown, also displayed in the notebook) | `Files/dea/reports/<run>/summary.md`, `kpis.json` | steering committee view: inventory, readiness, waves, top findings |
# | Excel workbook | `Files/dea/reports/<run>/dataflow_assessment.xlsx` (CSV files when pandas / openpyxl are missing) | working file for the migration team |
# | Lineage graph | `lineage.mmd` (Mermaid), `lineage.dot` (Graphviz), `lineage.svg` when the `dot` binary exists | the most connected linked dataflows and their consumers, coloured by readiness |
#
# The Delta tables are the system of record: point a Power BI semantic model (Direct Lake) at `Assessment_DataflowSummary`,
# `Assessment_MigrationReadiness`, `Assessment_UnsupportedFeatures`, `Assessment_WavePlan` and `Assessment_LineageNodes` / `Assessment_Lineage`.

# %%
import shutil
import subprocess

READINESS_ORDER = ["Ready", "Needs Attention", "Unsupported", "Deferred"]
WIZARD_ORDER = ["Ready to migrate", "Needs Attention", "Upgrade unavailable"]
BAND_ORDER = ["Low", "Medium", "High", "Very High"]
LEVEL_ORDER = ["High", "Medium", "Low"]
EXPORT_SHEETS = [("Dataflows", "Assessment_DataflowSummary"), ("Readiness", "Assessment_MigrationReadiness"), ("Findings", "Assessment_UnsupportedFeatures"),
                 ("Waves", "Assessment_WavePlan"), ("Backlog", "Assessment_MigrationBacklog"), ("Rebind", "Assessment_RebindWorklist"),
                 ("Sources", "Assessment_DataflowSources"), ("Lineage", "Assessment_Lineage"), ("Workspaces", "Assessment_Workspaces"),
                 ("Rules", "Assessment_RuleCatalog")]
REPORT_TABLES = sorted({name for _, name in EXPORT_SHEETS} | {"Assessment_Capacities", "Assessment_LineageNodes"})


def count_by(rows, key, order=None):
    counts = Counter(r.get(key) if r.get(key) not in (None, "") else "Unknown" for r in rows)
    ordered = list(order or [])
    return {k: counts.get(k, 0) for k in ordered + sorted(k for k in counts if k not in ordered)}


def pct(part, total):
    return round(100.0 * part / total, 1) if total else 0.0


def mean(values):
    values = [v for v in values if v is not None]
    return round(sum(values) / len(values), 1) if values else None


def compute_kpis(tables, top_n=25, run_id=None, now=None):
    summary, ready, findings = tables["Assessment_DataflowSummary"], tables["Assessment_MigrationReadiness"], tables["Assessment_UnsupportedFeatures"]
    rules = {r["rule_id"]: r for r in tables["Assessment_RuleCatalog"]}
    planned = [r for r in ready if r.get("readiness")]
    planned_ids = {r["dataflow_id"] for r in planned}
    summary_by_id = {r["dataflow_id"]: r for r in summary}
    planned_summary = [summary_by_id[i] for i in planned_ids if i in summary_by_id]
    migrated = [r for r in ready if r.get("migration_status") in ("Migrated", "MigratedBySaveAs")]
    families = defaultdict(set)
    for f in findings:
        if f["impact_wizard"] != "None" or f["impact_saveas"] != "None":
            families[f["rule_id"]].add(f["dataflow_id"])
    waves = defaultdict(lambda: {"dataflows": 0, "workspaces": 0, "ready": 0, "needs_attention": 0, "unsupported": 0, "deferred": 0, "effort_days": 0.0, "label": ""})
    for w in tables["Assessment_WavePlan"]:
        agg = waves[w["wave"]]
        agg["label"] = w["wave_label"]
        agg["workspaces"] += 1
        for key, col in (("dataflows", "dataflow_count"), ("ready", "ready_count"), ("needs_attention", "needs_attention_count"),
                         ("unsupported", "unsupported_count"), ("deferred", "deferred_count")):
            agg[key] += w[col] or 0
        agg["effort_days"] = round(agg["effort_days"] + (w["total_effort_days"] or 0.0), 2)
    consumers = lambda s: (s.get("downstream_dataset_count") or 0) + (s.get("downstream_report_count") or 0) + (s.get("downstream_dashboard_count") or 0)  # noqa: E731
    total_program = len(planned) + len(migrated)
    return {
        "run_id": run_id, "generated_at": now.isoformat(timespec="seconds") if now else None,
        "inventory": {"capacities": len(tables["Assessment_Capacities"]), "workspaces": len(tables["Assessment_Workspaces"]),
                      "workspaces_with_dataflows": len({s["workspace_id"] for s in summary}), "dataflows": len(summary),
                      "gen1": sum(1 for s in summary if s.get("dataflow_type") == "Gen1"), "gen2": sum(1 for s in summary if s.get("dataflow_type") == "Gen2"),
                      "unknown_generation": sum(1 for s in summary if s.get("dataflow_type") not in ("Gen1", "Gen2"))},
        "readiness": count_by(planned, "readiness", READINESS_ORDER), "wizard_prediction": count_by(planned, "predicted_wizard_status", WIZARD_ORDER),
        "paths": count_by(planned, "recommended_path"), "complexity": count_by(planned, "complexity_band", BAND_ORDER),
        "avg_complexity_score": mean(r.get("complexity_score") for r in planned), "effort_days_total": round(sum(r.get("effort_days") or 0.0 for r in planned), 1),
        "migration": {"in_program": total_program, "migrated": len(migrated), "remaining": len(planned), "progress_pct": pct(len(migrated), total_program),
                      "save_as_failed": sum(1 for r in ready if r.get("migration_status") == "SaveAsFailed")},
        "waves": [{"wave": k, **v} for k, v in sorted(waves.items())],
        "top_findings": [{"rule_id": rid, "name": rules.get(rid, {}).get("name", rid), "severity": rules.get(rid, {}).get("severity"),
                          "category": rules.get(rid, {}).get("category"), "dataflows": len(ids), "share_pct": pct(len(ids), len(planned))}
                         for rid, ids in sorted(families.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:top_n]],
        "top_complex": [{"dataflow": r.get("dataflow_id"), "name": summary_by_id.get(r["dataflow_id"], {}).get("dataflow_name"),
                         "workspace": summary_by_id.get(r["dataflow_id"], {}).get("workspace_name"), "score": r["complexity_score"], "band": r["complexity_band"],
                         "readiness": r["readiness"], "wave": r["wave"]}
                        for r in sorted(planned, key=lambda r: (-(r.get("complexity_score") or 0), r["dataflow_id"]))[:top_n]],
        "top_impact": [{"dataflow": s["dataflow_id"], "name": s.get("dataflow_name"), "workspace": s.get("workspace_name"), "models": s.get("downstream_dataset_count"),
                        "reports": s.get("downstream_report_count"), "dashboards": s.get("downstream_dashboard_count"), "readiness": s.get("readiness")}
                       for s in sorted(planned_summary, key=lambda s: (-consumers(s), s["dataflow_id"]))[:top_n] if consumers(s)],
        "environment": count_by(planned, "environment"), "criticality": count_by(planned, "business_criticality", LEVEL_ORDER),
        "capacity": count_by([{"capacity": s.get("capacity_name") or "(shared or unresolved)"} for s in planned_summary], "capacity"),
        "refresh": {"failing": sum(1 for s in planned_summary if s.get("refresh_ok") is False), "never_refreshed": sum(1 for s in planned_summary if s.get("last_refresh_status") is None),
                    "scheduled": sum(1 for s in planned_summary if s.get("schedule_enabled")), "avg_last_duration_min": mean((s.get("last_refresh_duration_sec") or 0) / 60.0
                                                                                                                     for s in planned_summary if s.get("last_refresh_duration_sec") is not None)},
        "connectivity": {"dataflows_with_gateway": sum(1 for s in planned_summary if s.get("uses_gateway")), "source_categories": count_by(tables["Assessment_DataflowSources"], "source_category")},
        "data_quality": {"confidence": count_by(planned, "assessment_confidence", LEVEL_ORDER), "definition_not_inspected": len(families.get("DAT-001", ())),
                         "unknown_generation": sum(1 for s in summary if s.get("dataflow_type") not in ("Gen1", "Gen2"))},
    }


def _cell(value):
    text = "" if value is None else (f"{value:.1f}" if isinstance(value, float) else str(value))
    return text.replace("|", "\\|").replace("\n", " ")


def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(_cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def dict_table(title, counts, total=None):
    total = total if total is not None else sum(counts.values())
    return md_table([title, "Dataflows", "Share %"], [(k, v, pct(v, total)) for k, v in counts.items()])


def render_markdown(k):
    inv, mig = k["inventory"], k["migration"]
    parts = [f"# Dataflow Gen1 to Gen2 migration assessment\nRun `{k['run_id']}` - generated {k['generated_at']}",
             "## Inventory\n" + md_table(["Capacities", "Workspaces", "With dataflows", "Dataflows", "Gen1", "Gen2", "Unknown"],
                                         [(inv["capacities"], inv["workspaces"], inv["workspaces_with_dataflows"], inv["dataflows"], inv["gen1"], inv["gen2"], inv["unknown_generation"])]),
             f"## Migration progress\n{mig['migrated']} migrated, {mig['remaining']} remaining ({mig['progress_pct']} % of {mig['in_program']}); "
             f"{mig['save_as_failed']} Save-As failures. Estimated remaining effort: **{k['effort_days_total']} person-days**, average complexity {k['avg_complexity_score']}.",
             "## Readiness of the remaining Gen1 dataflows\n" + dict_table("Readiness", k["readiness"]),
             "## Predicted Upgrade Wizard assessment\n" + dict_table("Wizard status", k["wizard_prediction"]),
             "## Recommended path\n" + dict_table("Path", k["paths"]), "## Complexity\n" + dict_table("Band", k["complexity"]),
             "## Migration waves\n" + md_table(["Wave", "Label", "Dataflows", "Workspaces", "Ready", "Needs Attention", "Unsupported", "Deferred", "Effort (days)"],
                                                [(w["wave"], w["label"], w["dataflows"], w["workspaces"], w["ready"], w["needs_attention"], w["unsupported"], w["deferred"], w["effort_days"]) for w in k["waves"]]),
             "## Most frequent findings\n" + md_table(["Rule", "Finding", "Severity", "Category", "Dataflows", "Share %"],
                                                      [(f["rule_id"], f["name"], f["severity"], f["category"], f["dataflows"], f["share_pct"]) for f in k["top_findings"]]),
             "## Most complex dataflows\n" + md_table(["Dataflow", "Workspace", "Score", "Band", "Readiness", "Wave"],
                                                      [(t["name"] or t["dataflow"], t["workspace"], t["score"], t["band"], t["readiness"], t["wave"]) for t in k["top_complex"]]),
             "## Highest downstream impact\n" + md_table(["Dataflow", "Workspace", "Models", "Reports", "Dashboards", "Readiness"],
                                                         [(t["name"] or t["dataflow"], t["workspace"], t["models"], t["reports"], t["dashboards"], t["readiness"]) for t in k["top_impact"]]),
             "## Governance\n" + dict_table("Environment", k["environment"]) + "\n\n" + dict_table("Business criticality", k["criticality"]) + "\n\n" + dict_table("Capacity", k["capacity"]),
             f"## Refresh and connectivity\nFailing refresh: {k['refresh']['failing']}; never refreshed: {k['refresh']['never_refreshed']}; scheduled: {k['refresh']['scheduled']}; "
             f"average last duration: {k['refresh']['avg_last_duration_min']} min; dataflows behind a gateway: {k['connectivity']['dataflows_with_gateway']}.\n\n"
             + dict_table("Source category", k["connectivity"]["source_categories"]),
             "## Data quality\n" + dict_table("Assessment confidence", k["data_quality"]["confidence"])
             + f"\n\nDefinitions not inspected: {k['data_quality']['definition_not_inspected']}; unknown generation: {k['data_quality']['unknown_generation']}."]
    return "\n\n".join(parts) + "\n"


def show_markdown(text):
    try:
        from IPython.display import Markdown, display
        display(Markdown(text))
    except Exception:  # noqa: BLE001 - outside a notebook the text is simply printed
        print(text)

# %%
def kpi_rows(kpis):
    """Flatten the KPI dictionary to (section, metric, value) rows for the Summary sheet."""
    rows = []
    for section, value in kpis.items():
        if isinstance(value, dict):
            for key, inner in value.items():
                if isinstance(inner, dict):
                    rows += [(f"{section}.{key}", k2, v2) for k2, v2 in inner.items()]
                else:
                    rows.append((section, key, inner))
        elif not isinstance(value, list):
            rows.append(("run", section, value))
    return rows


def excel_value(value):
    if isinstance(value, (list, tuple, set)):
        value = "; ".join(str(v) for v in value)
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    if isinstance(value, str):
        value = value[:32000]
        if value[:1] in ("=", "+", "-", "@", "\t", "\r"):
            value = "'" + value   # spreadsheet formula injection: tenant-controlled names must never become formulas
    return value


MAX_SHEET_ROWS = 200000   # larger detail tables go to a CSV file next to the workbook (Excel itself stops at 1 048 575 rows)


def sheet_view(tables, name):
    cols = [c[0] for c in TABLES[name].cols]
    return cols, [[excel_value(r.get(c)) for c in cols] for r in tables[name]]


def oversized_sheets(tables, limit=None):
    limit = MAX_SHEET_ROWS if limit is None else limit
    return [sheet for sheet, name in EXPORT_SHEETS if len(tables[name]) > limit]


def build_workbook(tables, kpis):
    """Excel workbook as bytes, or None when pandas / openpyxl are unavailable. A sheet with more than MAX_SHEET_ROWS rows only carries
    a note: its rows are written as CSV by export_reports."""
    try:
        import pandas as pd
        import openpyxl  # noqa: F401
    except ImportError:
        return None
    too_big = set(oversized_sheets(tables))
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([(a, b, excel_value(c)) for a, b, c in kpi_rows(kpis)], columns=["Section", "Metric", "Value"]).to_excel(writer, sheet_name="Summary", index=False)
        for sheet, name in EXPORT_SHEETS:
            if sheet in too_big:
                note = f"{len(tables[name])} rows: too many for a worksheet, see {sheet.lower()}.csv next to this workbook"
                pd.DataFrame([(note,)], columns=["Note"]).to_excel(writer, sheet_name=sheet, index=False)
                continue
            cols, data = sheet_view(tables, name)
            pd.DataFrame(data, columns=cols).to_excel(writer, sheet_name=sheet, index=False)
        for sheet in writer.book.worksheets:
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            for column in sheet.iter_cols(min_row=1, max_row=200):
                width = max((len(str(c.value)) for c in column if c.value is not None), default=8)
                sheet.column_dimensions[column[0].column_letter].width = min(60, width + 2)
    return buffer.getvalue()


def write_csv(files, rel, cols, data):
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(cols)
    writer.writerows(data)
    return files.write_bytes(rel, ("\ufeff" + out.getvalue()).encode("utf-8"))


def export_reports(ctx, tables, kpis, markdown):
    base = f"reports/{ctx.run_id}"
    written = [ctx.files.write_text(f"{base}/summary.md", markdown), ctx.files.write_json(f"{base}/kpis.json", kpis, compress=False)]
    workbook = build_workbook(tables, kpis)
    if workbook is not None:
        written.append(ctx.files.write_bytes(f"{base}/dataflow_assessment.xlsx", workbook))
        csv_sheets = oversized_sheets(tables)
    else:
        ctx.logger.warning("pandas / openpyxl not available: writing CSV files instead of the Excel workbook")
        csv_sheets = [sheet for sheet, _ in EXPORT_SHEETS]
    for sheet, name in EXPORT_SHEETS:
        if sheet in csv_sheets:
            cols, data = sheet_view(tables, name)
            written.append(write_csv(ctx.files, f"{base}/{sheet.lower()}.csv", cols, data))
            if workbook is not None:
                ctx.logger.warning("%s has %d rows: written as %s.csv, the workbook sheet only carries a note", sheet, len(data), sheet.lower())
    return written

# %%
NODE_FILL = {"Ready": "#c8e6c9", "Needs Attention": "#fff3cd", "Unsupported": "#f8d7da", "Deferred": "#e0e0e0"}
TYPE_FILL = {"Dataflow": "#e3f2fd", "SemanticModel": "#ede7f6", "Report": "#fce4ec", "Dashboard": "#f3e5f5", "DataPipeline": "#e8f5e9", "Datamart": "#fff8e1"}
CLASS_OF = {"Ready": "ready", "Needs Attention": "attention", "Unsupported": "unsupported", "Deferred": "deferred"}


def lineage_subgraph(nodes, edges, max_nodes):
    """The most connected linked dataflows first, then their neighbours and the reports behind their models, up to max_nodes.
    Data sources are left out (they are in Assessment_DataflowSources)."""
    by_id = {n["node_id"]: n for n in nodes}
    pairs = sorted({(e["src_id"], e["dst_id"]) for e in edges if e["src_type"] != "Datasource" and e["src_id"] in by_id and e["dst_id"] in by_id})
    neighbours, successors, linked = defaultdict(set), defaultdict(set), Counter()
    for src, dst in pairs:
        neighbours[src].add(dst)
        neighbours[dst].add(src)
        successors[src].add(dst)
        if by_id[src]["node_type"] == "Dataflow" and by_id[dst]["node_type"] == "Dataflow":
            linked[src] += 1
            linked[dst] += 1
    seeds = sorted((i for i, n in by_id.items() if n["node_type"] == "Dataflow" and neighbours[i]), key=lambda i: (-linked[i], -len(neighbours[i]), i))
    chosen = []
    for seed in seeds:
        group = [seed] + sorted(neighbours[seed])
        for hop in sorted(successors[seed]):
            if by_id[hop]["node_type"] != "Dataflow":
                group += sorted(successors[hop])
        for node_id in group:
            if node_id not in chosen and len(chosen) < max_nodes:
                chosen.append(node_id)
        if len(chosen) >= max_nodes:
            break
    keep = set(chosen)
    return [by_id[i] for i in chosen], [(s, d) for s, d in pairs if s in keep and d in keep]


def _safe(text, limit=44):
    return truncate(re.sub(r'["<>\[\]{}|`\\]', "'", str(text or "")), limit)


def _detail(node):
    detail = node["node_type"]
    if node.get("readiness"):
        detail += f" - {node['readiness']}" + (f" - wave {node['wave']}" if node.get("wave") is not None else "")
    elif node["node_type"] == "Dataflow" and node.get("generation") == 2:
        detail += " - Gen2"
    return detail


def to_mermaid(nodes, edges):
    ids = {n["node_id"]: f"n{i}" for i, n in enumerate(nodes)}
    lines = ["flowchart LR"]
    lines += [f'  {ids[n["node_id"]]}["{_safe(n["node_name"])}<br/><i>{_safe(_detail(n), 60)}</i>"]' for n in nodes]
    lines += [f"  {ids[s]} --> {ids[d]}" for s, d in edges]
    lines += [f"  classDef {c} fill:{NODE_FILL[r]},stroke:#555" for r, c in CLASS_OF.items()]
    for readiness, klass in CLASS_OF.items():
        members = [ids[n["node_id"]] for n in nodes if n.get("readiness") == readiness]
        if members:
            lines.append(f"  class {','.join(members)} {klass}")
    return "\n".join(lines) + "\n"


def to_dot(nodes, edges):
    ids = {n["node_id"]: f"n{i}" for i, n in enumerate(nodes)}
    lines = ["digraph lineage {", "  rankdir=LR;", '  node [shape=box, style="rounded,filled", fontname="Segoe UI", fontsize=10];']
    for n in nodes:
        fill = NODE_FILL.get(n.get("readiness")) or TYPE_FILL.get(n["node_type"], "#ffffff")
        lines.append(f'  {ids[n["node_id"]]} [label="{_safe(n["node_name"])}\\n{_safe(_detail(n), 60)}", fillcolor="{fill}"];')
    lines += [f"  {ids[s]} -> {ids[d]};" for s, d in edges]
    return "\n".join(lines + ["}"]) + "\n"


def render_svg(dot_text):
    """SVG bytes through the Graphviz `dot` binary when it exists in the runtime, else None."""
    binary = shutil.which("dot")
    if not binary:
        return None
    try:
        done = subprocess.run([binary, "-Tsvg"], input=dot_text.encode("utf-8"), capture_output=True, timeout=60, check=True)
        return done.stdout
    except (OSError, subprocess.SubprocessError):
        return None


def render_lineage(ctx, tables):
    nodes, edges = lineage_subgraph(tables["Assessment_LineageNodes"], tables["Assessment_Lineage"], ctx.cfg.lineage_graph_max_nodes)
    if not nodes:
        ctx.logger.info("no lineage to draw")
        return []
    base = f"reports/{ctx.run_id}"
    dot_text, mermaid = to_dot(nodes, edges), to_mermaid(nodes, edges)
    written = [ctx.files.write_text(f"{base}/lineage.mmd", mermaid), ctx.files.write_text(f"{base}/lineage.dot", dot_text)]
    svg = render_svg(dot_text)
    if svg:
        written.append(ctx.files.write_bytes(f"{base}/lineage.svg", svg))
        try:
            from IPython.display import SVG, display
            display(SVG(data=svg))
        except Exception:  # noqa: BLE001
            ctx.logger.info("lineage graph rendered to lineage.svg")
    else:
        ctx.logger.info("Graphviz `dot` binary not found: lineage written as Mermaid (lineage.mmd) and DOT (lineage.dot) only")
        show_markdown("### Lineage (Mermaid source)\n```mermaid\n" + mermaid + "```")
    return written


def phase_report(ctx):
    tables = {name: ctx.lake.read(name) for name in REPORT_TABLES}
    kpis = compute_kpis(tables, ctx.cfg.top_n, ctx.run_id, ctx.clock.now())
    markdown = render_markdown(kpis)
    ctx.state["kpis"] = kpis
    show_markdown(markdown)
    files = []
    if ctx.cfg.export_reports:
        files += export_reports(ctx, tables, kpis, markdown)
    if ctx.cfg.render_lineage_graph:
        files += render_lineage(ctx, tables)
    ctx.state["report_files"] = files
    return {"rows_in": kpis["inventory"]["dataflows"], "rows_out": len(files), "message": f"{len(files)} files written under reports/{ctx.run_id}"}

# %% [markdown]
# ### Run - section 9: reporting output

# %% tags=["run"]
run_steps(ctx, sections={9})
