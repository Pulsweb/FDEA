# %% [markdown]
# ## 1.6 Orchestration
# The notebook is a **pipeline of named steps** grouped in sections. Every section ends with a small run cell
# (`run_steps(ctx, sections={n})`), so the notebook can be executed top to bottom (*Run all*, a pipeline notebook activity,
# a schedule) or section by section while troubleshooting.
#
# | Run mode | Steps executed |
# |---|---|
# | `incremental` (default) | everything; the Scanner reads only workspaces modified since the last complete scan, collectors skip entities that are up to date |
# | `full` | everything; full Scanner pass and every definition is read again |
# | `resume` | like `incremental`, continues the run id in `p_resume_run_id` (log sequence and checkpoints carry on) |
# | `refresh_only` | refresh history and schedules only, then analysis, publish and report |
# | `analysis_only` | no API call: re-evaluates rules, scores and waves from the stored Silver tables (tune weights, rules or governance overrides) |
#
# A critical failure (authentication, access check) **aborts** the remaining steps; the logs are still persisted and the notebook fails at the end.

# %%
import tempfile

COLLECTION_MODES = ("full", "incremental", "resume")
LIVE_MODES = COLLECTION_MODES + ("refresh_only",)


@dataclasses.dataclass(frozen=True)
class Step:
    section: int
    name: str
    func: str       # resolved by name when the step runs: later sections are defined in later cells
    modes: tuple
    enabled: object
    critical: bool = False
    needs: tuple = ()   # phases whose output this step works on: it is skipped when one of them failed


def access_check(ctx):
    """Stop early when the identity can reach neither admin API family: nothing useful could be discovered."""
    access = ctx.state.get("access") or {}
    if access.get("pbi_admin") != "OK" and access.get("fabric_admin") != "OK":
        raise AuthError("neither the Power BI nor the Fabric admin APIs are accessible: " + (", ".join(f"{k}={v}" for k, v in access.items()) or "preflight did not complete"))
    return {"message": ", ".join(f"{k}={v}" for k, v in access.items())}


def pipeline_steps():
    always = lambda cfg: True  # noqa: E731
    return [
        Step(2, "preflight", "preflight", LIVE_MODES, always, critical=True),
        Step(2, "access_check", "access_check", COLLECTION_MODES, always, critical=True),
        Step(3, "governance", "phase_governance", RUN_MODES, always),
        Step(3, "capacities", "phase_capacities", COLLECTION_MODES, always),
        Step(3, "workspaces", "phase_workspaces", COLLECTION_MODES, always),
        Step(3, "scan", "phase_scan", COLLECTION_MODES, always),
        Step(4, "admin_dataflows", "phase_admin_dataflows", COLLECTION_MODES, always),
        Step(4, "admin_items", "phase_admin_items", COLLECTION_MODES, always),
        Step(4, "reconcile", "phase_reconcile_dataflows", COLLECTION_MODES, always),
        Step(5, "definitions", "phase_definitions", COLLECTION_MODES, lambda c: c.collect_definitions or c.collect_gen2_definitions),
        Step(5, "compute_engine", "phase_compute_engine", COLLECTION_MODES, lambda c: c.collect_compute_engine),
        Step(5, "admin_datasources", "phase_admin_datasources", COLLECTION_MODES, lambda c: c.collect_admin_datasources),
        Step(5, "upstream_verify", "phase_upstream_verify", COLLECTION_MODES, lambda c: c.verify_upstream_admin_api),
        Step(5, "pipelines", "phase_pipelines", COLLECTION_MODES, lambda c: c.collect_pipeline_references),
        Step(5, "connections", "phase_item_connections", COLLECTION_MODES, lambda c: c.collect_item_connections),
        Step(5, "owners_graph", "phase_owners_graph", COLLECTION_MODES, lambda c: c.enrich_owners_graph),
        Step(5, "source_rollup", "phase_source_rollup", COLLECTION_MODES, always),
        Step(6, "refresh", "phase_refresh_history", LIVE_MODES, lambda c: c.collect_refresh_history),
        Step(7, "analysis", "phase_analysis", RUN_MODES, always),
        Step(8, "publish", "phase_publish", RUN_MODES, always, needs=("analysis",)),
        Step(8, "maintenance", "phase_maintenance", RUN_MODES, lambda c: c.run_maintenance and not c.dry_run, needs=("publish",)),
        Step(9, "report", "phase_report", RUN_MODES, always, needs=("publish",)),
    ]


def run_steps(ctx, sections=None):
    """Run the steps of the given sections (all when None) that apply to the run mode and the parameters."""
    for step in pipeline_steps():
        if sections is not None and step.section not in sections:
            continue
        if ctx.aborted:
            ctx.logger.warning("run aborted - skipping %s", step.name)
            continue
        in_mode = ctx.cfg.run_mode in step.modes
        reason = "disabled by parameters" if in_mode else f"not part of run mode {ctx.cfg.run_mode}"
        broken = [n for n in step.needs if ctx.phase_status.get(n) == "Failed" or n in ctx.blocked_phases]
        if in_mode and broken:
            ctx.blocked_phases.add(step.name)
            reason = f"skipped because {', '.join(broken)} did not complete (the Gold tables could be inconsistent)"
        else:
            ctx.blocked_phases.discard(step.name)    # the marker describes the latest evaluation: a recovered step must not keep blocking its followers
        try:
            ctx.run_phase(step.name, globals()[step.func], enabled=in_mode and not broken and bool(step.enabled(ctx.cfg)), skip_reason=reason, critical=step.critical)
        except Exception as exc:  # noqa: BLE001 - run_phase re-raises critical failures; the run is aborted but its log is kept
            ctx.aborted, ctx.abort_reason = True, f"{step.name}: {type(exc).__name__}: {exc}"[:500]
            ctx.logger.error("critical step failed - the run is aborted: %s", ctx.abort_reason)
    return ctx.phase_status


def _try_import_notebookutils():
    try:
        import notebookutils
        return notebookutils
    except ImportError:
        return None


def _make_lake(cfg, ns, ctx, clock, logger):
    if cfg.dry_run:
        logger.info("dry run: results stay in memory, nothing is written to the Lakehouse")
        return InMemoryLake(ctx.run_id, clock.now)
    spark = ns.get("spark")
    if spark is None:
        raise ConfigError("no Spark session found: run the notebook in Fabric, or set p_dry_run = True for an in-memory run")
    return SparkLake(spark, cfg.lakehouse_schema, ctx.run_id, logger, clock.now)


def _make_files(cfg):
    mounted = "/lakehouse/default"
    if cfg.dry_run:
        return FileStore(os.path.join(tempfile.mkdtemp(prefix="dea-dry-"), "dea"), mounted if os.path.isdir(mounted) else None)
    if not os.path.isdir(mounted):
        raise ConfigError("attach a default Lakehouse to the notebook: /lakehouse/default is not mounted")
    return FileStore.for_lakehouse(cfg.files_root)


def start_run(ns, deps=None):
    """Sections 1-2: validate the parameters, then wire logging, storage and identity. `deps` lets tests inject collaborators."""
    deps = deps or {}
    cfg = deps.get("cfg") or Config.from_namespace(ns)
    logger, capture = setup_logging(cfg.log_level)
    clock = deps.get("clock") or SystemClock()
    resumed = cfg.run_mode == "resume"
    ctx = RunContext(cfg, run_id=cfg.resume_run_id if resumed else None, clock=clock, logger=logger)
    ctx.log_capture, ctx.aborted, ctx.abort_reason = capture, False, None
    logger.info("Dataflow assessment %s | rules %s | run %s | mode=%s | dry_run=%s", NOTEBOOK_VERSION, RULES_VERSION, ctx.run_id, cfg.run_mode, cfg.dry_run)
    ctx.lake = deps.get("lake") or _make_lake(cfg, ns, ctx, clock, logger)
    ctx.lake.run_id = ctx.run_id
    ctx.lake.ensure_tables()
    ctx.files = deps.get("files") or _make_files(cfg)
    logger.info("files root: %s", ctx.files.root)
    ctx.checkpoints = CheckpointStore(ctx.lake, cfg, clock)
    loaded = ctx.checkpoints.load()
    if resumed:
        ctx.continue_sequence(max((r["seq"] or 0 for r in ctx.lake.read("Assessment_RunLog", where={"run_id": ctx.run_id})), default=0))
    if cfg.run_mode != "analysis_only":
        session = deps.get("session") or ThreadSessions()
        tokens = deps.get("tokens") or resolve_identity(cfg, ns.get("notebookutils") or _try_import_notebookutils(), session, clock, logger)
        build_clients(ctx, tokens, session=session)
    logger.info("ready: %d checkpoints loaded", loaded)
    return ctx


def exit_notebook(ns, summary):
    """Hand the outcome to a calling pipeline (notebookutils.notebook.exit). A no-op outside Fabric."""
    nbutils = ns.get("notebookutils") or _try_import_notebookutils()
    if nbutils is None:
        return summary
    keys = ("run_id", "status", "mode", "dataflows_total", "ready_count", "needs_attention_count", "unsupported_count", "deferred_count", "total_effort_days")
    nbutils.notebook.exit(json.dumps({k: summary.get(k) for k in keys}, default=str))
    return summary
