# %% [markdown]
# # 10. Logging and monitoring
# | Signal | Where | Notes |
# |---|---|---|
# | Structured console log | notebook output | `time | level | run id | phase | message`; run id and phase come from a logging filter |
# | Phase log | `Assessment_RunLog` | status, duration, rows, API calls / retries / throttling, errors per phase |
# | Error log | `Assessment_ErrorLog` | endpoint template, HTTP status, error code, truncated body; never tokens or secrets |
# | Run KPIs | `Assessment_RunSummary` | one row per run: counts by readiness, effort, API usage - ready for trend and burn-down charts |
# | Run report | `Files/dea/runs/<run id>.json` | everything above plus API statistics per bucket, access preflight and recent warnings |
# | Pipeline signal | notebook exit value, `p_fail_on_partial` | a pipeline can branch on `status` or fail the activity |
#
# Status semantics: **Succeeded** (all phases fine), **Partial** (a phase stopped early on the deadline, API budget or throttling:
# the next run resumes from the checkpoints), **Failed** (a phase raised). A critical failure (authentication, preflight) aborts the run.

# %%
class RunFailed(DeaError):
    """Raised at the very end of a run, after the logs are persisted, so pipelines see the failure."""


def build_run_summary(ctx, status):
    kpis = ctx.state.get("kpis") or {}
    stats = (ctx.state.get("analysis") or {}).get("stats") or {}
    inventory, readiness = kpis.get("inventory") or {}, kpis.get("readiness") or stats.get("classes") or {}
    totals = ctx.stats.totals()
    return {
        "run_id": ctx.run_id, "started_at": ctx.started_at, "ended_at": utcnow(), "status": status, "mode": ctx.cfg.run_mode,
        "workspaces_total": inventory.get("workspaces"), "workspaces_scanned": len(ctx.state.get("scanned_ws_ids") or ()),
        "dataflows_total": inventory.get("dataflows") if inventory else stats.get("dataflows"), "gen1_count": inventory.get("gen1"), "gen2_count": inventory.get("gen2"),
        "ready_count": readiness.get("Ready"), "needs_attention_count": readiness.get("Needs Attention"), "unsupported_count": readiness.get("Unsupported"),
        "deferred_count": readiness.get("Deferred"), "migrated_count": (kpis.get("migration") or {}).get("migrated", stats.get("migrated")),
        "avg_complexity_score": kpis.get("avg_complexity_score"), "total_effort_days": kpis.get("effort_days_total", stats.get("effort_days")),
        "api_calls": totals["calls"], "api_throttled": totals["throttled"], "error_count": len(ctx.error_rows), "notebook_version": NOTEBOOK_VERSION}


def run_log_rows(ctx):
    rows = [dict(r) for r in ctx.phase_rows]
    if rows:
        rows[0]["params_json"] = json.dumps(ctx.cfg.public_dict(), default=str, sort_keys=True)
    return rows


def run_report(ctx, status, summary):
    capture = getattr(ctx, "log_capture", None)
    return {"run_id": ctx.run_id, "status": status, "mode": ctx.cfg.run_mode, "dry_run": ctx.cfg.dry_run, "notebook_version": NOTEBOOK_VERSION,
            "rules_version": RULES_VERSION, "summary": summary, "phases": ctx.phase_rows, "access": ctx.state.get("access"),
            "api": {"totals": ctx.stats.totals(), "by_bucket": ctx.stats.by_bucket()}, "errors": ctx.error_rows[:200],
            "warnings": [list(w) for w in list(capture.records)[-100:]] if capture else [], "parameters": ctx.cfg.public_dict()}


def log_banner(ctx, status):
    log = ctx.logger.info
    log("=" * 98)
    log("run %s finished: %s (mode=%s%s)", ctx.run_id, status.upper(), ctx.cfg.run_mode, ", dry run" if ctx.cfg.dry_run else "")
    log("%-18s %-10s %8s %9s %9s %6s %5s %4s", "phase", "status", "sec", "rows in", "rows out", "api", "429", "err")
    for r in ctx.phase_rows:
        log("%-18s %-10s %8.1f %9s %9s %6s %5s %4s", r["phase"], r["status"], r.get("duration_sec") or 0.0, r.get("rows_in") or "-", r.get("rows_out") or "-",
            r.get("api_calls"), r.get("api_throttled"), r.get("errors"))
    totals = ctx.stats.totals()
    log("API: %s calls, %s retries, %s throttled (HTTP 429), %s failed; %s errors logged", totals["calls"], totals["retries"], totals["throttled"], totals["failures"], len(ctx.error_rows))
    for bucket, s in ctx.stats.by_bucket().items():
        log("   %-32s calls=%-5s retries=%-4s throttled=%-4s failed=%-4s avg=%sms", bucket, s["calls"], s["retries"], s["throttled"], s["failures"], s["avg_ms"])
    log("=" * 98)


def _monitor_state(ctx):
    """What finalize_run already did in this context, so that running it again (a retry of the last cell) completes the work instead of repeating it."""
    return ctx.state.setdefault("monitor", {"run_log": 0, "error_log": 0})


def attempt_number(ctx):
    """1 for a normal run, then 2, 3 ... for each resume of the same run id (counted from the reports already written); fixed per context."""
    state = _monitor_state(ctx)
    if "attempt" not in state:
        try:
            written = [f for f in ctx.files.list("runs") if os.path.basename(f).startswith(ctx.run_id)]
        except Exception:  # noqa: BLE001 - the number only names the report file
            written = []
        state["attempt"] = len(written) + 1
    return state["attempt"]


def cumulative_summary(ctx, summary):
    """A resumed run continues one KPI row: earliest start, cumulative API usage and error count. The earlier attempts are read once per
    context, so persisting again replaces this attempt's share instead of adding it a second time."""
    if ctx.cfg.run_mode != "resume":
        return summary
    state = _monitor_state(ctx)
    if "prior" not in state:
        try:
            rows = ctx.lake.read("Assessment_RunSummary", where={"run_id": ctx.run_id})
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("could not read the previous KPI row of the resumed run: %s", exc)
            return summary
        state["prior"] = rows[0] if rows else None
    prior = state["prior"]
    if not prior:
        return summary
    merged = dict(summary)
    merged["started_at"] = prior.get("started_at") or summary["started_at"]
    for key in ("api_calls", "api_throttled", "error_count"):
        merged[key] = (prior.get(key) or 0) + (summary.get(key) or 0)
    return merged


def guarded(ctx, label, action, problems):
    """Run one persistence step on its own: a failure is logged and collected, and never prevents the other steps."""
    try:
        action()
    except Exception as exc:  # noqa: BLE001
        problems.append(f"{label}: {type(exc).__name__}: {exc}"[:300])
        ctx.logger.error("could not persist the %s: %s: %s", label, type(exc).__name__, exc)


def append_new(ctx, table, rows, counter):
    """Append the rows that this context has not stored yet (the lists only grow)."""
    state = _monitor_state(ctx)
    fresh = rows[state[counter]:]
    if fresh:
        ctx.lake.append(table, fresh)
    state[counter] = len(rows)


def finalize_run(ctx):
    """Persist checkpoints, run log, error log, KPIs and the JSON report, each step independently. Never raises: monitoring must not hide
    the run outcome; what could not be stored is returned in summary['persistence_problems'] and written to the report. Calling it again
    stores what is still missing, and rewrites the KPI row and the report of the same attempt."""
    status = "Failed" if getattr(ctx, "aborted", False) else ctx.overall_status()
    summary = cumulative_summary(ctx, build_run_summary(ctx, status))
    problems = []
    guarded(ctx, "checkpoints", ctx.checkpoints.flush, problems)
    guarded(ctx, "run log", lambda: append_new(ctx, "Assessment_RunLog", run_log_rows(ctx), "run_log"), problems)
    guarded(ctx, "error log", lambda: append_new(ctx, "Assessment_ErrorLog", list(ctx.error_rows), "error_log"), problems)
    guarded(ctx, "run summary", lambda: ctx.lake.merge("Assessment_RunSummary", [summary]), problems)
    attempt = attempt_number(ctx)
    report = {**run_report(ctx, status, summary), "attempt": attempt, "persistence_problems": problems}
    guarded(ctx, "run report", lambda: ctx.files.write_json(f"runs/{ctx.run_id}{'' if attempt == 1 else f'.attempt{attempt}'}.json", report, compress=False), problems)
    log_banner(ctx, status)
    return {**summary, "persistence_problems": problems}


def finish_run(ctx):
    """Section 10: persist everything, then fail the notebook if the run aborted (or, with p_fail_on_partial, did not fully succeed or
    could not be recorded)."""
    summary = finalize_run(ctx)
    if getattr(ctx, "aborted", False):
        raise RunFailed(f"run {ctx.run_id} aborted: {getattr(ctx, 'abort_reason', 'critical phase failed')}")
    if ctx.cfg.fail_on_partial and summary["status"] != "Succeeded":
        raise RunFailed(f"run {ctx.run_id} finished {summary['status']}: see Assessment_RunLog and Assessment_ErrorLog")
    if ctx.cfg.fail_on_partial and summary["persistence_problems"]:
        raise RunFailed(f"run {ctx.run_id} could not be fully recorded: " + "; ".join(summary["persistence_problems"]))
    return summary

# %% [markdown]
# ### Run - section 10: logging and monitoring
# Persists the run log, error log, KPIs and the JSON report, prints the run banner, returns the outcome to a calling pipeline
# and fails the notebook when the run aborted (or, with `p_fail_on_partial`, did not fully succeed).

# %% tags=["run"]
SUMMARY = finish_run(ctx)
exit_notebook(globals(), SUMMARY)
