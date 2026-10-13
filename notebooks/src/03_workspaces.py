# %% [markdown]
# # 3. Workspace discovery
# | Step | API | Why |
# |---|---|---|
# | Governance overrides | CSV in `Files/dea/config` (+ table) | Business owner, criticality, support team, environment, defer / pin-wave, observed wizard status |
# | Capacities | Power BI `admin/capacities` (fallback Fabric `capacities`) | Name, SKU, Fabric capability. The list can be incomplete - unresolved capacities get an explicit bucket |
# | Workspaces | Fabric `admin/workspaces` + Power BI `admin/groups` | State, capacity, domain, tags + attached-storage (BYOSA) and deployment-pipeline ids |
# | Domains / environments | Fabric `admin/domains`, Power BI `admin/pipelines` | Classification and Dev / Test / Prod detection (pipeline stage, then naming convention) |
# | Scanner | `admin/workspaces/modified` -> `getInfo` -> `scanStatus` -> `scanResult` | Bulk metadata and lineage for 100 workspaces per call; incremental through `modifiedSince` |
#
# Notes: the Scanner is limited to 500 `getInfo` calls/hour and **16 concurrent scans**; scan results expire after 24 h;
# `modifiedSince` must be between 30 minutes and 30 days. The watermark only advances after a *complete* scan.

# %%
_CRITICALITY = {"high": "High", "h": "High", "critical": "High", "critique": "High", "élevée": "High", "elevee": "High", "élevé": "High",
                "eleve": "High", "haute": "High", "medium": "Medium", "m": "Medium", "moyenne": "Medium", "moyen": "Medium",
                "low": "Low", "l": "Low", "faible": "Low", "basse": "Low"}
_ENVIRONMENT = {"dev": "Dev", "development": "Dev", "developpement": "Dev", "développement": "Dev", "test": "Test", "tst": "Test",
                "uat": "Test", "qa": "Test", "recette": "Test", "prod": "Prod", "prd": "Prod", "production": "Prod"}
_WIZARD = {"ready": "Ready to migrate", "ready to migrate": "Ready to migrate", "needs attention": "Needs Attention",
           "needs_attention": "Needs Attention", "upgrade unavailable": "Upgrade unavailable", "unavailable": "Upgrade unavailable",
           "not supported": "Upgrade unavailable", "unsupported": "Upgrade unavailable"}


def norm_criticality(value):
    return _CRITICALITY.get((value or "").strip().lower())


def norm_environment(value):
    return _ENVIRONMENT.get((value or "").strip().lower())


def norm_wizard_status(value):
    return _WIZARD.get((value or "").strip().lower())


def parse_governance_csv(text):
    """Parse and validate the governance CSV (English or French labels). Returns (rows, problems)."""
    rows, problems = [], []
    for line_no, raw in enumerate(csv.DictReader(io.StringIO(text.lstrip("\ufeff"))), start=2):
        rec = {(k or "").strip().lower(): (v or "").strip() for k, v in raw.items()}
        ws, df = gid(rec.get("workspace_id")), gid(rec.get("dataflow_id"))
        if not ws and not df:
            problems.append(f"line {line_no}: workspace_id or dataflow_id is required")
            continue
        crit = norm_criticality(rec.get("business_criticality"))
        env = norm_environment(rec.get("environment"))
        wizard = norm_wizard_status(rec.get("wizard_status_actual"))
        for field, supplied, parsed in (("business_criticality", rec.get("business_criticality"), crit),
                                        ("environment", rec.get("environment"), env),
                                        ("wizard_status_actual", rec.get("wizard_status_actual"), wizard)):
            if supplied and not parsed:
                problems.append(f"line {line_no}: unknown {field} {supplied!r} ignored")
        rows.append({
            "scope": "Dataflow" if df else "Workspace", "workspace_id": ws or "", "dataflow_id": df or "",
            "business_owner": rec.get("business_owner") or None, "technical_owner": rec.get("technical_owner") or None,
            "support_team": rec.get("support_team") or None, "business_criticality": crit,
            "workspace_classification": rec.get("workspace_classification") or None, "environment": env,
            "defer": _as_bool(rec.get("defer")) if rec.get("defer") else None, "defer_reason": rec.get("defer_reason") or None,
            "pin_wave": coerce_value("INT", rec.get("pin_wave")) if rec.get("pin_wave") else None, "wizard_status_actual": wizard,
            "rollback_plan_defined": _as_bool(rec.get("rollback_plan_defined")) if rec.get("rollback_plan_defined") else None,
            "uat_window_start": coerce_value("DATE", rec.get("uat_window_start")), "uat_window_end": coerce_value("DATE", rec.get("uat_window_end")),
            "notes": rec.get("notes") or None, "updated_by": rec.get("updated_by") or None,
            "updated_at": coerce_value("TIMESTAMP", rec.get("updated_at")) or utcnow(),
        })
    return rows, problems


def governance_index(rows):
    """Index overrides by dataflow and by workspace. Several rows can address the same target (the table is keyed by scope, workspace id and
    dataflow id): the most recently updated wins and exact ties are broken by content, so the order of a Delta read never changes the outcome."""
    def precedence(row):
        return (row.get("updated_at") or dt.datetime.min, json.dumps(row, sort_keys=True, default=str))

    index = {"workspace": {}, "dataflow": {}}
    for row in sorted(rows, key=precedence):
        if row.get("scope") == "Dataflow" and row.get("dataflow_id"):
            index["dataflow"][gid(row["dataflow_id"])] = row
        elif row.get("workspace_id"):
            index["workspace"][gid(row["workspace_id"])] = row
    return index


def phase_governance(ctx):
    """Merge the optional governance CSV into Assessment_GovernanceOverrides and index it for later phases."""
    cfg = ctx.cfg
    try:
        path = ctx.files.lakehouse_path(cfg.governance_overrides_path)
    except ValueError as exc:
        ctx.logger.warning("governance path ignored: %s", exc)
        path = None
    loaded = 0
    if path and os.path.isfile(path):
        with open(path, "r", encoding="utf-8-sig") as handle:
            rows, problems = parse_governance_csv(handle.read())
        for problem in problems:
            ctx.logger.warning("governance CSV: %s", problem)
        if rows:
            ctx.lake.merge("Assessment_GovernanceOverrides", rows)
            loaded = len(rows)
    else:
        ctx.logger.info("no governance CSV at %s - using the table content only", cfg.governance_overrides_path)
    table_rows = ctx.lake.read("Assessment_GovernanceOverrides")
    ctx.state["governance"] = governance_index(table_rows)
    return {"rows_in": loaded, "rows_out": len(table_rows), "message": f"{loaded} rows loaded from CSV, {len(table_rows)} active overrides"}

# %%
def phase_capacities(ctx):
    cfg, pbi, fabric = ctx.cfg, ctx.clients["pbi"], ctx.clients["fabric"]
    source = "admin"
    try:
        items = list(pbi.paginate("admin/capacities", buckets=("pbi_admin_capacities",), items_key="value"))
    except (ForbiddenError, AuthError) as exc:
        ctx.logger.warning("admin/capacities denied (%s): using Fabric capacities, which only lists capacities this identity can access", exc.status)
        items, source = list(fabric.paginate("capacities", buckets=("fabric_platform",), items_key="value")), "core"
    rows = {}
    for c in items:
        cid = gid(c.get("id"))
        if not cid:
            continue
        kind, capable = classify_sku(c.get("sku"))
        rows[cid] = {"capacity_id": cid, "capacity_name": c.get("displayName"), "sku": c.get("sku"), "capacity_kind": kind,
                     "is_fabric_capable": capable, "region": c.get("region"), "state": c.get("state"), "admins": c.get("admins") or [],
                     "source": source}
    ctx.state["capacities"] = rows
    ctx.state["source_capacity_id"] = None
    if cfg.source_capacity:
        wanted = cfg.source_capacity.strip().lower()
        match = [cid for cid, r in rows.items() if cid == wanted or (r["capacity_name"] or "").lower() == wanted]
        if not match:
            raise ConfigError(f"capacity {cfg.source_capacity!r} not found among {len(rows)} visible capacities")
        ctx.state["source_capacity_id"] = match[0]
        ctx.logger.info("scope limited to capacity %s (%s)", rows[match[0]]["capacity_name"], match[0])
    ctx.lake.merge("Assessment_Capacities", list(rows.values()))
    return {"rows_in": len(items), "rows_out": len(rows), "message": f"source={source}"}

# %%
def _stage_info(pipelines):
    """workspace id -> (pipeline id, stage order, stage count) from admin/pipelines?$expand=stages."""
    info = {}
    for pipe in pipelines:
        stages = pipe.get("stages") or []
        for stage in stages:
            wid = gid(stage.get("workspaceId"))
            if wid:
                info[wid] = (gid(pipe.get("id")), stage.get("order"), len(stages))
    return info


def resolve_workspace_attributes(name, pipeline_stage, override, domain_name, patterns):
    """Environment (override > pipeline stage > naming) and classification (override > domain)."""
    env, env_source = "Unknown", "Unknown"
    if override and override.get("environment"):
        env, env_source = override["environment"], "Override"
    elif pipeline_stage and stage_environment(pipeline_stage[1], pipeline_stage[2]) != "Unknown":
        env, env_source = stage_environment(pipeline_stage[1], pipeline_stage[2]), "DeploymentPipeline"
    else:
        guessed = detect_environment(name, patterns)
        if guessed != "Unknown":
            env, env_source = guessed, "NamingConvention"
    classification = (override or {}).get("workspace_classification") or domain_name or "Unclassified"
    return env, env_source, classification


def build_workspace_row(fab, grp, capacities, domains, stages, governance, patterns):
    wid = gid(fab.get("id") or grp.get("id"))
    name = fab.get("name") or grp.get("name")
    wtype = fab.get("type") or grp.get("type")
    cap_id = gid(fab.get("capacityId") or grp.get("capacityId"))
    cap = capacities.get(cap_id) if cap_id else None
    if not cap_id:
        resolution, capable = "Shared", False
    elif cap:
        resolution, capable = "Resolved", cap.get("is_fabric_capable")
    else:
        resolution, capable = "Unresolved", None
    dedicated = grp.get("isOnDedicatedCapacity")
    domain_id = gid(fab.get("domainId"))
    domain_name = domains.get(domain_id)
    override = governance["workspace"].get(wid)
    stage = stages.get(wid)
    env, env_source, classification = resolve_workspace_attributes(name, stage, override, domain_name, patterns)
    tags = [t.get("displayName") if isinstance(t, dict) else str(t) for t in (fab.get("tags") or [])]
    return {
        "workspace_key": stable_hash64(wid), "workspace_id": wid, "workspace_name": name, "workspace_type": wtype,
        "state": fab.get("state") or grp.get("state"), "is_personal": str(wtype).lower() in ("personal", "personalgroup"),
        "description": grp.get("description"), "capacity_id": cap_id, "capacity_name": (cap or {}).get("capacity_name"),
        "capacity_sku": (cap or {}).get("sku"), "capacity_resolution": resolution,
        "is_on_dedicated_capacity": bool(cap_id) if dedicated is None else bool(dedicated), "is_fabric_capable": capable,
        "domain_id": domain_id, "domain_name": domain_name, "default_dataset_storage_format": grp.get("defaultDatasetStorageFormat"),
        "has_workspace_level_settings": grp.get("hasWorkspaceLevelSettings"), "dataflow_storage_id": grp.get("dataflowStorageId"),
        "pipeline_id": gid(grp.get("pipelineId")) or (stage[0] if stage else None), "pipeline_stage_order": stage[1] if stage else None,
        "environment": env, "environment_source": env_source, "workspace_classification": classification,
        "support_team": (override or {}).get("support_team"), "business_criticality": (override or {}).get("business_criticality"),
        "tags": [t for t in tags if t],
    }


def make_scope_filter(cfg, source_capacity_id):
    only = {x.strip().lower() for x in cfg.only_workspaces}
    excluded = {x.strip().lower() for x in cfg.exclude_workspaces}

    def keep(row):
        name, wid = (row.get("workspace_name") or "").lower(), row["workspace_id"]
        if only and name not in only and wid not in only:
            return False
        if name in excluded or wid in excluded:
            return False
        if source_capacity_id and row.get("capacity_id") != source_capacity_id:
            return False
        if row.get("is_personal") and not cfg.include_personal_workspaces:
            return False
        if row.get("state") != "Active" and not cfg.include_inactive_workspaces:
            return False
        return True

    return keep


def listing_excludes(cfg, row):
    """True when the configured listing cannot contain this stored workspace (personal and non-active ones are left out unless asked
    for): its absence says nothing about the tenant, so it must not be taken for a deletion."""
    if row.get("is_personal") and not cfg.include_personal_workspaces:
        return True
    return row.get("state") not in (None, "Active") and not cfg.include_inactive_workspaces


def phase_workspaces(ctx):
    cfg, pbi, fabric = ctx.cfg, ctx.clients["pbi"], ctx.clients["fabric"]
    fabric_ws, groups, fabric_ok, groups_ok = {}, {}, True, True
    kinds = ["Workspace"] + (["Personal"] if cfg.include_personal_workspaces else [])
    states = ["Active"] + (["Deleted"] if cfg.include_inactive_workspaces else [])
    try:
        for kind in kinds:
            for state in states:
                for w in fabric.paginate("admin/workspaces", params={"type": kind, "state": state}, items_key="workspaces",
                                         buckets=("fabric_admin_workspaces", "fabric_platform")):
                    fabric_ws[gid(w.get("id"))] = w
    except (ForbiddenError, AuthError) as exc:
        fabric_ok = False
        ctx.logger.warning("Fabric admin/workspaces denied (%s): relying on Power BI admin/groups only (no domain or tags)", exc.status)
    try:
        for g in pbi.paginate("admin/groups", style="skip", page_size=5000, buckets=("pbi_admin_groups",)):
            if g.get("state") in (None, "Active") or cfg.include_inactive_workspaces:
                groups[gid(g.get("id"))] = g
    except (ForbiddenError, AuthError) as exc:
        groups_ok = False
        ctx.logger.warning("Power BI admin/groups denied (%s): attached-storage and pipeline info unavailable", exc.status)
    if not fabric_ok and not groups_ok:
        raise ApiError("no workspace inventory API is accessible: check the tenant settings and the service principal group", status=403)

    domains = {}
    if fabric_ok:
        try:
            domains = {gid(d.get("id")): d.get("displayName") for d in
                       fabric.paginate("admin/domains", params={"preview": "false"}, items_key="domains", buckets=("fabric_admin_domains", "fabric_platform"))}
        except ApiError as exc:
            ctx.logger.warning("domains unavailable (%s)", exc.status)
    stages = {}
    try:
        stages = _stage_info(pbi.paginate("admin/pipelines", params={"$expand": "stages"}, style="skip", page_size=5000, buckets=("pbi_admin_pipelines",)))
    except ApiError as exc:
        ctx.logger.warning("deployment pipelines unavailable (%s): environment falls back to naming conventions", exc.status)

    patterns = {**DEFAULT_ENV_PATTERNS, **cfg.environment_patterns}
    governance = ctx.state.get("governance") or {"workspace": {}, "dataflow": {}}
    rows = {}
    for wid in set(fabric_ws) | set(groups):
        if wid:
            rows[wid] = build_workspace_row(fabric_ws.get(wid, {}), groups.get(wid, {}), ctx.state["capacities"], domains, stages, governance, patterns)
    known = ctx.lake.read("Assessment_Workspaces", include_deleted=True)
    live = [r for r in known if not r.get("is_deleted")]
    if not rows and live:
        raise ApiError(f"both workspace listings came back empty although {len(live)} workspaces are known: not taken for a tenant-wide deletion", status=200,
                       endpoint="admin/workspaces", retryable=True)
    keep = make_scope_filter(cfg, ctx.state.get("source_capacity_id"))
    scope = {wid for wid, r in rows.items() if keep(r)}
    ctx.state.update(workspaces=rows, scope_ws_ids=scope, workspace_listing_complete=fabric_ok or groups_ok)

    # a workspace that was flagged deleted and is listed again is revived by the merge: in the same write, what was stored for it is
    # reset, so that it is scanned again (nobody needs to have modified it) and its sources and lineage come back with it
    was_deleted = {r["workspace_id"] for r in known if r.get("is_deleted")}
    ctx.lake.merge("Assessment_Workspaces", [r for w, r in rows.items() if w not in was_deleted])
    if was_deleted & set(rows):
        ctx.lake.merge("Assessment_Workspaces", [{**r, "scan_state": "NotScanned"} for w, r in rows.items() if w in was_deleted])
    active_ids = {wid for wid, r in rows.items() if r["state"] == "Active"}
    gone = [{"workspace_id": r["workspace_id"]} for r in live if r["workspace_id"] not in rows and not listing_excludes(cfg, r)]
    if gone:
        ctx.lake.mark_deleted("Assessment_Workspaces", gone)
    unresolved = sum(1 for r in rows.values() if r["capacity_resolution"] == "Unresolved")
    if unresolved:
        ctx.logger.warning("%d workspaces reference a capacity that no capacity API returned (kept as Unresolved)", unresolved)
    by_capacity = Counter(r["capacity_id"] for r in rows.values() if r["capacity_id"])
    caps = ctx.state["capacities"]
    for cid, count in by_capacity.items():
        row = caps.setdefault(cid, {"capacity_id": cid, "source": "derived", "capacity_kind": "Unknown", "admins": []})
        row["workspace_count"] = count
    ctx.lake.merge("Assessment_Capacities", [{"capacity_id": cid, "workspace_count": by_capacity.get(cid, 0), "source": r.get("source")}
                                             for cid, r in caps.items()], update_cols=["workspace_count", "source"])
    return {"rows_in": len(rows), "rows_out": len(scope), "message": f"{len(active_ids)} active, {len(scope)} in scope, {unresolved} unresolved capacity"}

# %%
SCAN_STATUS_DONE, SCAN_STATUS_FAILED = "Succeeded", "Failed"
MAX_SCAN_SPLITS = 40   # a failed batch is retried as two halves so that one bad workspace cannot sink its batch-mates; the budget bounds the extra calls


class ScanBatchError(ApiError):
    """The Scanner itself gave up on a batch (status Failed, or no result in time): usually a single workspace is the cause."""


def _iso_z(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def scan_fingerprint(ctx):
    """What a watermark covers: the scope and the Scanner flags. A watermark is only reused for an identical fingerprint,
    so a narrower run can never hide changes from a later, wider one."""
    cfg = ctx.cfg
    parts = {"only": sorted(x.strip().lower() for x in cfg.only_workspaces), "exclude": sorted(x.strip().lower() for x in cfg.exclude_workspaces),
             "capacity": ctx.state.get("source_capacity_id") or "", "personal": bool(cfg.include_personal_workspaces),
             "inactive": bool(cfg.include_inactive_workspaces), "users": bool(cfg.scan_get_users), "expressions": bool(cfg.scan_dataset_expressions)}
    return short_hash(json.dumps(parts, sort_keys=True), 12)


def mark_scan_state(ctx, workspace_ids, state, strict=False):
    """Record scan_state, so that the next run picks the workspaces up again. With strict=True a storage failure stops the caller:
    a plan must never go on, or certify anything, on states it could not write."""
    if not workspace_ids:
        return
    try:
        ctx.lake.merge("Assessment_Workspaces", [{"workspace_id": w, "scan_state": state} for w in workspace_ids])
    except Exception as exc:  # noqa: BLE001
        if strict:
            raise
        ctx.logger.error("could not record scan_state=%s for %d workspaces: %s: %s", state, len(workspace_ids), type(exc).__name__, exc)


def plan_scan(ctx, scope_ids, scan_started):
    """Return (workspace ids to scan, plan label).

    Incremental runs scan modified + never-scanned/failed workspaces. The watermark is the last *complete* scan of the same
    scope and Scanner flags. A different scope or other flags restart the scan from scratch, tracked per workspace (scan_state)
    and by a `Reset` checkpoint that names what it restarts for, so that the restart itself can be resumed - and is itself
    restarted when the configuration changes again. Without a usable watermark the oldest per-workspace scan time is used, so a
    restarted run does not repeat the batches that already succeeded.

    scan_state = Scanned means "current as of the watermark": what is stored for a workspace outside the scope is not, so it is
    reset, and the workspace is scanned again when it comes back.
    """
    cfg, pbi = ctx.cfg, ctx.clients["pbi"]
    ids = sorted(scope_ids)
    stored = {r["workspace_id"]: r for r in ctx.lake.read("Assessment_Workspaces")}
    uncovered = sorted(w for w, r in stored.items() if r.get("scan_state") == "Scanned" and w not in scope_ids)
    if uncovered:
        mark_scan_state(ctx, uncovered, "NotScanned", strict=True)
        stored.update({w: {**stored[w], "scan_state": "NotScanned"} for w in uncovered})
    watermark = None
    if cfg.run_mode != "full":
        fingerprint = scan_fingerprint(ctx)
        cp = ctx.checkpoints.get("Tenant", "scanner", "scan")
        stamp, _, recorded = str((cp or {}).get("source_version") or "").partition("|")
        if cp and cp.get("status") in ("Done", "Reset") and recorded != fingerprint:
            # the states first, then the checkpoint: when the states cannot be written the old checkpoint stays and the reset is retried
            mark_scan_state(ctx, ids, "NotScanned", strict=True)
            ctx.checkpoints.mark("Tenant", "scanner", "scan", "Reset", version=f"|{fingerprint}")
            ctx.checkpoints.flush()
            return ids, "full (the scope or the Scanner flags changed since the last complete scan)"
        if cp and cp.get("status") == "Done":
            watermark = parse_ts(stamp)
        if watermark is None:
            times = [r["scanned_at"] for w, r in stored.items() if w in scope_ids and r.get("scan_state") == "Scanned" and r.get("scanned_at")]
            watermark = min(times) if times else None
    if watermark is None or scan_started - watermark > dt.timedelta(days=cfg.scan_lookback_days):
        return ids, "full"
    since = min(watermark - dt.timedelta(hours=1), scan_started - dt.timedelta(minutes=31))
    since = max(since, scan_started - dt.timedelta(days=cfg.scan_lookback_days))
    params = {"modifiedSince": _iso_z(since), "excludePersonalWorkspaces": str(not cfg.include_personal_workspaces).lower()}
    if cfg.include_inactive_workspaces:
        params["excludeInActiveWorkspaces"] = "false"   # otherwise the service default applies and the scope filter drops non-active workspaces
    modified = pbi.get_json("admin/workspaces/modified", params=params, buckets=("pbi_scan_modified",))
    modified_ids = {gid(w.get("id")) for w in (modified if isinstance(modified, list) else modified.get("value", []))}
    pending = {wid for wid in scope_ids if (stored.get(wid) or {}).get("scan_state") != "Scanned"}
    chosen = sorted((modified_ids & set(scope_ids)) | pending)
    return chosen, f"incremental ({len(modified_ids & set(scope_ids))} modified, {len(pending)} unscanned or failed)"


def scan_one_batch(ctx, batch):
    """Worker: getInfo -> poll -> result. Pure API work, safe to run in threads."""
    cfg, pbi = ctx.cfg, ctx.clients["pbi"]
    flags = {"lineage": "true", "datasourceDetails": "true", "getArtifactUsers": str(cfg.scan_get_users).lower(),
             "datasetSchema": "false", "datasetExpressions": str(cfg.scan_dataset_expressions).lower()}
    started = pbi.post_json("admin/workspaces/getInfo", {"workspaces": batch}, params=flags, buckets=("pbi_scan_getinfo",), ok=(200, 202))
    scan_id = started["id"]
    give_up = ctx.clock.monotonic() + cfg.scan_timeout_sec
    while True:
        status = pbi.get_json(f"admin/workspaces/scanStatus/{scan_id}", buckets=("pbi_scan_status",)).get("status")
        if status == SCAN_STATUS_DONE:
            break
        if status == SCAN_STATUS_FAILED:
            raise ScanBatchError(f"scan {scan_id} failed", endpoint="admin/workspaces/scanStatus")
        if ctx.clock.monotonic() > give_up:
            raise ScanBatchError(f"scan {scan_id} did not finish within {cfg.scan_timeout_sec}s", endpoint="admin/workspaces/scanStatus", retryable=True)
        ctx.clock.sleep(cfg.scan_poll_interval_sec)
    return pbi.get_json(f"admin/workspaces/scanResult/{scan_id}", buckets=("pbi_scan_result",))


def phase_scan(ctx):
    cfg = ctx.cfg
    scope = ctx.state["scope_ws_ids"]
    scan_started = ctx.clock.now()
    ids, plan = plan_scan(ctx, scope, scan_started)
    ctx.logger.info("scanner plan: %d of %d workspaces (%s)", len(ids), len(scope), plan)
    batches = list(chunked(ids, cfg.scan_batch_size))
    ctx.state.setdefault("scanned_ws_ids", set())
    states = {wid: r.get("state") for wid, r in ctx.state["workspaces"].items()}
    failed_batches = missing_active = splits = results = 0
    stop, issued = None, len(batches)
    pending = deque(enumerate(batches, start=1))
    with cf.ThreadPoolExecutor(max_workers=cfg.scan_max_concurrent) as pool:
        inflight = {}
        while pending or inflight:
            while pending and len(inflight) < cfg.scan_max_concurrent and stop is None and not ctx.deadline.expired(reserve_sec=120):
                number, batch = pending.popleft()
                inflight[pool.submit(scan_one_batch, ctx, batch)] = (number, batch)
            if not inflight:
                break
            done, _ = cf.wait(list(inflight), return_when=cf.FIRST_COMPLETED)
            for future in done:
                number, batch = inflight.pop(future)
                try:
                    result = future.result()
                except GracefulStop as exc:
                    stop = exc
                    pending.appendleft((number, batch))
                    continue
                except Exception as exc:  # noqa: BLE001 - one bad batch must not lose the others
                    ctx.record_error("scan", "ScanBatch", f"batch-{number}", getattr(exc, "endpoint", None), exc=exc,
                                     message=str(exc) if len(batch) > 1 else f"{exc} (workspace {batch[0]})")
                    if isinstance(exc, ScanBatchError) and len(batch) > 1 and splits < MAX_SCAN_SPLITS:
                        splits += 1
                        half = (len(batch) + 1) // 2
                        pending.appendleft((issued + 2, batch[half:]))
                        pending.appendleft((issued + 1, batch[:half]))
                        issued += 2
                        ctx.logger.warning("scan batch %d failed (%s): retrying its %d workspaces as two smaller batches", number, exc, len(batch))
                    else:
                        failed_batches += 1
                        mark_scan_state(ctx, batch, "Failed")
                    continue
                try:
                    if cfg.persist_raw:
                        ctx.files.write_json(f"raw/{ctx.run_id}/scanner/batch-{number:04d}.json.gz", result)
                    parsed = parse_scan_result(ctx, result, batch)
                    persist_scan(ctx, parsed)
                except Exception as exc:  # noqa: BLE001
                    failed_batches += 1
                    ctx.record_error("scan", "ScanBatch", f"batch-{number}", "scanResult", exc=exc)
                    ctx.logger.error("could not ingest scan batch %d: %s: %s\n%s", number, type(exc).__name__, exc, "".join(traceback.format_exception(exc, limit=-4)))
                    mark_scan_state(ctx, batch, "Failed")
                    continue
                results += len(parsed["dataflows"])
                ctx.state["scanned_ws_ids"].update(parsed["scanned_workspace_ids"])
                # a workspace the Scanner silently left out is only normal when it is no longer active
                absent = [w for w in parsed["missing_workspace_ids"] if states.get(w, "Active") == "Active"]
                mark_scan_state(ctx, absent, "Failed")
                mark_scan_state(ctx, [w for w in parsed["missing_workspace_ids"] if w not in absent], "NotScanned")
                missing_active += len(absent)
                for wid in absent:
                    ctx.record_error("scan", "Workspace", wid, "scanResult", message="the workspace was requested but is missing from the scan result")
    if stop is None and pending:
        stop = DeadlineExceeded(f"deadline reached with {len(pending)} of {issued} scan batches left")
    complete = not pending and failed_batches == 0 and missing_active == 0 and stop is None
    if complete:
        ctx.checkpoints.mark("Tenant", "scanner", "scan", "Done", version=f"{_iso_z(scan_started)}|{scan_fingerprint(ctx)}")
        ctx.checkpoints.flush()
    if stop is not None:
        raise stop
    if cfg.persist_raw and cfg.raw_retention_days:
        ctx.files.purge_older_than("raw", cfg.raw_retention_days)
    healthy = failed_batches == 0 and missing_active == 0
    return {"rows_in": len(ids), "rows_out": results, "status": "Succeeded" if healthy else "Partial",
            "message": f"{issued} batches ({splits} split after a failure), {failed_batches} failed, {missing_active} workspaces missing from the results, plan={plan}"}

# %% [markdown]
# ### Run - section 3: workspace discovery

# %% tags=["run"]
run_steps(ctx, sections={3})
