# %% [markdown]
# # 6. Refresh history collection
# | Generation | API | Notes |
# |---|---|---|
# | Gen1 | `groups/{ws}/dataflows/{id}/transactions` | Needs workspace access (an SPN must be a member); no admin variant exists |
# | Gen2 | `workspaces/{ws}/items/{id}/jobs/instances` | Fabric keeps about the 100 most recent completed runs; `failureReason` carries the error |
# | Gen2 schedules | `items/{id}/jobs/{jobType}/schedules` (`Refresh`, then `Execute`) | Gen1 schedules come from the Scanner (there is no read API) |
#
# Every record is normalized (status, refresh type, duration) and merged into `Assessment_RefreshHistory`
# (partitioned by month). Per-dataflow KPIs - success rate, average and p95 duration, **consecutive failures**, last error,
# API-triggered runs - are computed from the observed window and drive the *Deferred* class (failing refresh) and the
# refresh-criticality score. Work is budgeted per run and refreshed after `p_refresh_history_ttl_hours`.

# %%
_STATUS = {
    "Success": ("success", "succeeded", "completed", "complete", "succeededwithwarnings"),
    "Failed": ("failed", "failure", "error", "faulted", "timedout"),
    "Cancelled": ("cancelled", "canceled", "cancelling"),
    "InProgress": ("inprogress", "in progress", "running", "notstarted", "not started", "queued", "accepted", "pending", "executing"),
    "Skipped": ("deduped", "skipped"),
}
_EXTERNAL_TYPES = {"viaapi", "external", "api"}
REFRESH_COLS = ["last_refresh_at", "last_refresh_status", "last_refresh_duration_sec", "last_success_at", "refresh_runs_observed", "refresh_success_rate",
                "refresh_avg_duration_sec", "refresh_p95_duration_sec", "consecutive_failures", "last_error_message", "external_trigger_seen",
                "refresh_history_status", "refresh_history_collected_at"]
SCHEDULE_COLS = ["schedule_enabled", "schedule_days", "schedule_times", "schedule_runs_per_day", "schedule_timezone", "schedule_notify_option"]
WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def normalize_status(raw):
    text = str(raw or "").strip().lower().replace("_", "")
    for status, names in _STATUS.items():
        if text in names:
            return status
    return "Unknown"


def build_history_row(df, record, source_api):
    """Normalize one Gen1 transaction or Gen2 job instance. Returns None for records that never started."""
    if source_api == "PBI.transactions":
        rid, rtype, raw = record.get("id"), record.get("refreshType"), record.get("status")
        start, end = parse_ts(record.get("startTime")), parse_ts(record.get("endTime"))
        code, message = None, record.get("errorMessage") or record.get("error")
    else:
        rid, rtype, raw = record.get("id"), record.get("invokeType"), record.get("status")
        start, end = parse_ts(record.get("startTimeUtc")), parse_ts(record.get("endTimeUtc"))
        failure = record.get("failureReason") or {}
        code, message = failure.get("errorCode"), failure.get("message")
    if not rid or start is None:
        return None
    status = normalize_status(raw)
    duration = (end - start).total_seconds() if end and end >= start else None
    return {"dataflow_id": df["dataflow_id"], "refresh_id": str(rid), "workspace_id": df["workspace_id"], "dataflow_type": df["dataflow_type"],
            "source_api": source_api, "refresh_type": rtype, "status": raw, "status_normalized": status, "start_time": start, "end_time": end,
            "duration_sec": duration, "error_code": code, "error_message": truncate(message, 2000) if message else None, "start_date": start.date(),
            "start_month": start.strftime("%Y-%m")}


def compute_refresh_metrics(rows, now):
    """KPIs from the observed window (rows are Assessment_RefreshHistory dicts)."""
    ordered = sorted(rows, key=lambda r: r["start_time"], reverse=True)
    finished = [r for r in ordered if r["status_normalized"] in ("Success", "Failed")]
    successes = [r for r in finished if r["status_normalized"] == "Success"]
    streak = 0
    for r in ordered:
        if r["status_normalized"] == "Failed":
            streak += 1
        elif r["status_normalized"] == "Success":
            break
    last = ordered[0] if ordered else None
    last_failed = next((r for r in ordered if r["status_normalized"] == "Failed"), None)
    last_done = next((r for r in ordered if r["status_normalized"] in ("Success", "Failed", "Cancelled") and r["duration_sec"] is not None), None)
    durations = [r["duration_sec"] for r in finished if r["duration_sec"] is not None]
    success_durations = [r["duration_sec"] for r in successes if r["duration_sec"] is not None]
    return {
        "last_refresh_at": last["start_time"] if last else None, "last_refresh_status": last["status_normalized"] if last else None,
        "last_refresh_duration_sec": last_done["duration_sec"] if last_done else None,
        "last_success_at": max((r["end_time"] or r["start_time"] for r in successes), default=None),
        "refresh_runs_observed": len(ordered), "refresh_success_rate": (len(successes) / len(finished)) if finished else None,
        "refresh_avg_duration_sec": (sum(durations) / len(durations)) if durations else None,
        "refresh_p95_duration_sec": percentile(success_durations or durations, 0.95), "consecutive_failures": streak,
        "last_error_message": last_failed["error_message"] if last_failed else None,
        "external_trigger_seen": any(str(r.get("refresh_type") or "").lower() in _EXTERNAL_TYPES for r in ordered),
        "refresh_history_status": "Collected", "refresh_history_collected_at": now,
    }


def parse_gen2_schedules(schedules):
    """Combine the enabled Fabric schedules of a dataflow into the Silver schedule columns."""
    items = [s for s in schedules or [] if isinstance(s, dict)]
    if not items:
        return {"schedule_enabled": False, "schedule_days": [], "schedule_times": [], "schedule_runs_per_day": 0}
    enabled = [s for s in items if s.get("enabled")]
    runs, times, days, tz = 0, [], set(), None
    for sched in enabled:
        cfg = sched.get("configuration") or {}
        kind = str(cfg.get("type") or "").lower()
        tz = tz or cfg.get("localTimeZoneId")
        if kind == "cron":
            interval = max(1, int(cfg.get("interval") or 1440))
            runs += max(1, round(1440 / interval))
            days.update(WEEK)
        else:
            slots = [str(t) for t in cfg.get("times") or []]
            runs += len(slots)
            times += slots
            days.update(cfg.get("weekdays") or WEEK)
    return {"schedule_enabled": bool(enabled), "schedule_days": [d for d in WEEK if d in days], "schedule_times": sorted(set(times)),
            "schedule_runs_per_day": runs, "schedule_timezone": tz}

# %%
def fetch_refresh(ctx, row):
    """Worker: read the refresh window (and the Gen2 schedule) of one dataflow."""
    cfg, wid, did = ctx.cfg, row["workspace_id"], row["dataflow_id"]
    schedule, schedule_denied = None, False
    if row["dataflow_type"] == "Gen2":
        fabric = ctx.clients["fabric"]
        records = list(fabric.paginate(f"workspaces/{wid}/items/{did}/jobs/instances", items_key="value", buckets=("fabric_jobs", "fabric_platform")))
        records = [r for r in records if str(r.get("jobType") or "refresh").lower() in ("refresh", "execute")]
        source = "Fabric.jobInstances"
        for job_type in ("Refresh", "Execute"):
            try:
                schedule = parse_gen2_schedules(list(fabric.paginate(f"workspaces/{wid}/items/{did}/jobs/{job_type}/schedules", items_key="value",
                                                                     buckets=("fabric_jobs", "fabric_platform"))))
                break
            except NotFoundError:
                continue
            except (ForbiddenError, AuthError) as exc:
                # the job history is still valid: keep it, but make the missing schedule visible (schedule_enabled stays NULL = unknown)
                schedule_denied = True
                ctx.record_error("refresh", "DataflowSchedule", did, exc.endpoint, exc=exc)
                break
    else:
        body = ctx.clients["pbi"].get_json(f"groups/{wid}/dataflows/{did}/transactions", buckets=("pbi_dataflow",))
        records = body.get("value", []) if isinstance(body, dict) else body
        source = "PBI.transactions"
    rows = [r for r in (build_history_row(row, rec, source) for rec in records) if r]
    rows.sort(key=lambda r: r["start_time"], reverse=True)
    rows = rows[:cfg.refresh_history_max_records]
    return {"rows": rows, "metrics": compute_refresh_metrics(rows, ctx.clock.now()), "schedule": schedule, "schedule_denied": schedule_denied}


def persist_refresh(ctx, outcomes):
    """Returns (history rows written, Gen2 schedules that could not be read)."""
    history, updates, schedules_denied = [], [], 0
    for row, result, status in outcomes:
        if result is None:
            updates.append({"dataflow_id": row["dataflow_id"], "refresh_history_status": "AccessDenied" if status == "AccessDenied" else "Failed"})
            continue
        history += result["rows"]
        schedules_denied += bool(result.get("schedule_denied"))
        update = {"dataflow_id": row["dataflow_id"], **result["metrics"]}
        if result["schedule"]:
            update.update({k: result["schedule"].get(k) for k in SCHEDULE_COLS})
        updates.append(update)
    if history:
        months = {r["start_month"] for r in history}
        ctx.lake.merge("Assessment_RefreshHistory", history, prune={"start_month": months})
    merge_by_signature(ctx.lake, "Assessment_Dataflows", updates)
    return len(history), schedules_denied


def phase_refresh_history(ctx):
    cfg = ctx.cfg
    wanted = lambda r: ((r["dataflow_type"] == "Gen1" and cfg.collect_gen1) or (r["dataflow_type"] == "Gen2" and cfg.collect_gen2))  # noqa: E731
    candidates, eligible = select_candidates(ctx, "refresh", wanted, cfg.budget_refresh_dataflows_per_run, ttl_hours=cfg.refresh_history_ttl_hours)
    ctx.logger.info("refresh history: %d due, %d selected (budget %d dataflows)", eligible, len(candidates), cfg.budget_refresh_dataflows_per_run)
    written = []

    def ok(row, result):
        ctx.checkpoints.mark("Dataflow", row["dataflow_id"], "refresh", "Done")

    def persist(outcomes):
        written.append(persist_refresh(ctx, outcomes))

    done = run_budgeted(ctx, "refresh", candidates, lambda row: fetch_refresh(ctx, row), ok, persist)
    denied = sum(1 for r in ctx.error_rows if r["phase"] == "refresh" and r["entity_type"] == "Dataflow" and r["http_status"] in (401, 403))
    runs, schedules_denied = sum(w[0] for w in written), sum(w[1] for w in written)
    if denied:
        ctx.logger.warning("%d refresh histories could not be read (HTTP 401/403): the service principal must be a member of those workspaces "
                           "(Gen1 transactions have no admin API); the Deferred and criticality rules treat them as unknown", denied)
    if schedules_denied:
        ctx.logger.warning("%d Gen2 schedules could not be read (HTTP 401/403): their schedule_enabled stays unknown (see Assessment_ErrorLog)", schedules_denied)
    left = max(0, eligible - len(candidates))
    return {"rows_in": eligible, "rows_out": runs, "status": "Partial" if left else "Succeeded",
            "message": f"{done} dataflows, {runs} runs written, {denied} denied, {schedules_denied} schedules denied, {left} left for the next run"}

# %% [markdown]
# ### Run - section 6: refresh history and schedules

# %% tags=["run"]
run_steps(ctx, sections={6})
