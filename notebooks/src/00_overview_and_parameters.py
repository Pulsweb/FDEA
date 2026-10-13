# %% [markdown]
# # Dataflow Estate Assessment (DEA)
# **Tenant-wide inventory and migration-readiness assessment for Power BI Dataflow Gen1 and Fabric Dataflow Gen2.**
#
# The notebook harvests metadata with Power BI / Fabric REST APIs (service principal preferred), persists a
# dimensional model into Delta tables of the attached Lakehouse, then scores every dataflow for a Gen1 -> Gen2
# migration: readiness class, complexity score, effort, recommendation and migration wave.
#
# | # | Section | What it does |
# |---|---|---|
# | 1 | Configuration | Parameters, schema registry, rule catalog, run context, logging, storage layer |
# | 2 | Authentication | Service principal / notebook identity tokens, rate-limited resilient API client, preflight |
# | 3 | Workspace Discovery | Capacities, workspaces, domains, environments, Scanner API (incremental) |
# | 4 | Dataflow Discovery | Gen1 + Gen2 inventory, sources, lineage edges, reconciliation |
# | 5 | Dependency Collection | Definitions (model.json / mashup), M analysis, extra lineage, owners |
# | 6 | Refresh History Collection | Gen1 transactions, Gen2 job instances, schedules, refresh KPIs |
# | 7 | Migration Readiness Analysis | Rules, complexity score, readiness, dependency closure, waves |
# | 8 | Delta Persistence | Publish Gold layer, verify, maintain tables |
# | 9 | Reporting Output | KPIs, wave plan, exports (CSV/XLSX), lineage graph |
# | 10 | Logging and Monitoring | Run summary, API statistics, error report, status |
#
# **Execution model.** Cells are written to be run top to bottom (or as a pipeline notebook activity).
# Collectors persist to Delta after every micro-batch and record checkpoints, so a failed or time-boxed run can
# simply be re-run: finished work is skipped and API budgets are spent on what is missing or changed.
# The storage layer used by the collectors is defined in section 1.4; section 8 publishes and maintains the Gold layer.

# %% [markdown]
# ## Prerequisites
# 1. **Lakehouse** attached as the default Lakehouse of this notebook (schema-enabled recommended; default schema `dbo`).
# 2. **Service principal** (recommended) stored in Azure Key Vault - secrets: tenant id, client id, client secret.
#    * Fabric admin portal > Tenant settings: *Service principals can access read-only admin APIs*,
#      *Service principals can call Fabric public APIs*, and for rich Scanner output *Enhance admin APIs responses with
#      detailed metadata* (+ *...with DAX and mashup expressions*). Scope each to a security group that contains the SPN.
#    * The app registration must **not** have admin-consent Power BI API permissions configured.
#    * Optional deep inspection (definitions, refresh history) needs the SPN as workspace member; without it those
#      items are recorded as `AccessDenied` and the assessment degrades gracefully (confidence is lowered).
# 3. Fallback `p_auth_mode = "user"` uses the identity running the notebook (must be a Fabric administrator).
# 4. Optional governance input: `Files/dea/config/governance_overrides.csv` (see docs/04-runbook.md).
#
# **Never put secrets in this notebook.** Only Key Vault secret *names* are parameters.

# %% tags=["parameters"]
# ---------------------------------------------------------------------------------------------------------------
# Parameters (pipeline-overridable). Lists accept comma/semicolon separated text; dicts accept JSON text.
# ---------------------------------------------------------------------------------------------------------------
# Run control
p_run_mode = "incremental"          # full | incremental | resume | analysis_only | refresh_only
p_resume_run_id = ""                # run id to continue when p_run_mode = "resume"
p_dry_run = False                   # collect + analyse without writing Delta tables
p_max_runtime_minutes = 170         # soft deadline: stop starting new work, flush, finish gracefully
p_log_level = "INFO"
p_fail_on_partial = False           # raise at the end when a phase failed (useful for pipelines)

# Storage
p_lakehouse_schema = "dbo"          # "" for Lakehouses without schemas
p_files_root = "Files/dea"
p_persist_raw = True                # keep raw Scanner JSON in Files for audit / reprocessing
p_raw_retention_days = 30
p_run_maintenance = True            # OPTIMIZE (+ optional VACUUM) at the end
p_vacuum_retention_hours = 0        # 0 = skip VACUUM

# Authentication
p_auth_mode = "auto"                # auto | spn | user
p_key_vault_url = ""                # https://<vault>.vault.azure.net/
p_kv_secret_tenant_id = "dea-tenant-id"
p_kv_secret_client_id = "dea-client-id"
p_kv_secret_client_secret = "dea-client-secret"
p_authority_host = "https://login.microsoftonline.com"
p_pbi_api_base = "https://api.powerbi.com/v1.0/myorg"
p_fabric_api_base = "https://api.fabric.microsoft.com/v1"
p_graph_api_base = "https://graph.microsoft.com/v1.0"

# Scope
p_only_workspaces = ""              # names or ids; empty = whole tenant
p_exclude_workspaces = ""
p_source_capacity = ""              # capacity name or id; empty = all
p_include_personal_workspaces = False
p_include_inactive_workspaces = False

# Collection switches
p_collect_gen1 = True
p_collect_gen2 = True
p_collect_definitions = True        # model.json (Gen1) - deep inspection, API-budgeted
p_definition_source = "admin"       # admin | workspace: which API reads Gen1 definitions first. The admin export is capped at 200 requests/hour per
                                    # principal; "workspace" (identity is a workspace member) is far faster and falls back to the admin export
p_collect_gen2_definitions = False  # getDefinition needs read+write on the item (Contributor+)
p_collect_compute_engine = True     # workspace dataflow list: enhanced compute engine setting (needs workspace access)
p_collect_refresh_history = True
p_collect_admin_datasources = False # admin/dataflows/{id}/datasources for dataflows the Scanner did not describe
p_verify_upstream_admin_api = False # cross-check upstream links with admin upstreamDataflows (200 req/h)
p_collect_pipeline_references = False
p_collect_item_connections = False  # Fabric items/{id}/connections (Gen2, needs read+write)
p_enrich_owners_graph = False       # Microsoft Graph owner enrichment (needs User.Read.All)

# Scanner API
p_scan_batch_size = 100             # API maximum is 100
p_scan_max_concurrent = 8           # API maximum is 16
p_scan_poll_interval_sec = 10
p_scan_timeout_sec = 900
p_scan_lookback_days = 28           # incremental window (API allows 30 min .. 30 days)
p_scan_get_users = True
p_scan_dataset_expressions = False

# Throttling / budgets
p_max_workers = 4
p_rate_limit_scale = 0.9            # use 90% of documented hourly limits
p_rate_limit_overrides = ""         # JSON, e.g. {"pbi_admin_dataflow_export": [[100, 3600]]}
p_max_retries = 6
p_retry_base_delay_sec = 2.0
p_retry_max_delay_sec = 120.0
p_max_retry_after_sec = 900         # a longer Retry-After ends the phase (resume later)
p_budget_definitions_per_run = 400    # dataflows whose definition is read per run (the budgets bound the work per run, one to three API calls each)
p_budget_refresh_dataflows_per_run = 1500   # dataflows whose refresh history (and schedule) is collected per run
p_budget_datasources_per_run = 400
p_refresh_history_ttl_hours = 24
p_refresh_history_max_records = 100
p_denied_retry_days = 7
p_failed_retry_days = 1
p_max_attempts_per_entity = 3
p_batch_flush_size = 100

# Analysis
p_stale_days = 90                   # no successful refresh for N days and no consumers -> Deferred
p_failing_consecutive = 5           # N consecutive failed refreshes -> Deferred (fix first)
p_wizard_available = True           # Upgrade Wizard (in-place, preview) may be recommended
p_preferred_path = "auto"           # auto | wizard | saveas
p_require_definition_for_ready = True
p_pilot_size = 10
p_score_weights = ""                # JSON override of default weights
p_governance_overrides_path = "Files/dea/config/governance_overrides.csv"
p_migration_results_table = "dataflowmigrations"   # optional output table of the Save-As accelerator
p_environment_patterns = ""         # JSON override: {"Dev": "regex", "Test": "regex", "Prod": "regex"}

# Reporting
p_export_reports = True
p_top_n = 25
p_render_lineage_graph = True
p_lineage_graph_max_nodes = 60
