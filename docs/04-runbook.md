# Runbook

How to prepare, deploy, run, schedule and troubleshoot the assessment notebook, and how to use its output to drive the migration.

## 1. Prerequisites

| Item | Detail |
|---|---|
| Fabric workspace and capacity | any Fabric capacity (F2+ is enough: the workload is API-bound) |
| Lakehouse | attached as the **default Lakehouse** of the notebook; schema-enabled recommended (`dbo`); `p_lakehouse_schema = ""` for a legacy Lakehouse |
| Identity | a **service principal** (recommended), or a Fabric administrator running the notebook (`p_auth_mode = "user"`) |
| Key Vault | holds the service principal secrets; the identity running the notebook needs the *Key Vault Secrets User* role (or a get-secret access policy) |
| Runtime | Fabric Spark runtime with Python 3.9 or later; `requests`, `pandas` and `openpyxl` are part of the runtime (Excel export falls back to CSV without them); optional `dot` binary for SVG lineage |

### Service principal and tenant settings

1. Create an app registration and a client secret. **Do not** grant Power BI API permissions with admin consent: that disables the admin APIs for service principals.
2. Create an Entra **security group** containing the service principal.
3. Fabric admin portal > *Tenant settings*, enable and scope to that group:
   * *Service principals can access read-only admin APIs* (Power BI admin and Scanner APIs; Fabric admin APIs).
   * *Service principals can call Fabric public APIs* (Gen2 items, job instances, pipelines).
   * *Enhance admin APIs responses with detailed metadata* and *...with DAX and mashup expressions* (richer Scanner payloads: data source details, expressions).
4. Optional deep inspection:
   * Add the service principal as **Member or Contributor** of the workspaces whose Gen1 refresh history you need (the transactions API has no admin variant) and, for Gen2
     `getDefinition`, as Contributor or above (read and write on the item are required; the documentation on service principal support for this API is contradictory, so
     it is best effort).
   * Microsoft Graph application permission `User.Read.All` for `p_enrich_owners_graph` (owner account disabled or missing).
5. Store three secrets in Key Vault: tenant id, client id, client secret (default names `dea-tenant-id`, `dea-client-id`, `dea-client-secret`; change them with `p_kv_secret_*`).

Without service principal, `p_auth_mode = "user"` uses the notebook identity; the user must be a Fabric administrator. `p_auth_mode = "auto"` (default) uses the service principal when
`p_key_vault_url` is set, otherwise the notebook identity.

## 2. Deployment

The notebook is generated: `python tools/build_notebook.py` produces `notebooks/Dataflow_Assessment.ipynb`, `sql/lakehouse_ddl.sql`, `docs/data-dictionary.md` and `docs/rule-catalog.md`.
Optionally bind the default Lakehouse in the notebook metadata:

```text
python tools/build_notebook.py --lakehouse-id <lakehouse guid> --lakehouse-name <name> --workspace-id <workspace guid>
```

Then deploy the `.ipynb` with any Fabric route: *Import notebook* in the workspace, the Fabric CLI, the Fabric REST API (items API with the notebook definition), the VS Code
Fabric extension, or Git integration (commit the generated file, sync the workspace). Attach the Lakehouse if you did not bind it, set `p_key_vault_url`, and run.

## 3. Parameters

All parameters are in the first code cell (tagged `parameters`, so a pipeline notebook activity can override them). Lists accept comma, semicolon or newline separated text (or a
JSON array); dictionaries accept JSON text.

| Group | Parameter (default) | Purpose |
|---|---|---|
| Run control | `p_run_mode` (`incremental`) | `full`, `incremental`, `resume`, `refresh_only`, `analysis_only` |
| | `p_resume_run_id` | run id to continue with `resume` |
| | `p_dry_run` (`False`) | collect and analyse in memory, write nothing to the Lakehouse |
| | `p_max_runtime_minutes` (170) | soft deadline: stop starting new work, flush and finish as Partial |
| | `p_log_level`, `p_fail_on_partial` | console verbosity; raise at the end unless the run Succeeded (pipelines) |
| Storage | `p_lakehouse_schema` (`dbo`), `p_files_root` (`Files/dea`) | table schema; file root, a folder below `Files/` (validated: no `..`, no drive) |
| | `p_persist_raw`, `p_raw_retention_days` (30) | keep raw Scanner JSON and for how long |
| | `p_run_maintenance`, `p_vacuum_retention_hours` (0 = no VACUUM, otherwise at least 168) | OPTIMIZE and VACUUM |
| Authentication | `p_auth_mode` (`auto`), `p_key_vault_url`, `p_kv_secret_tenant_id`, `p_kv_secret_client_id`, `p_kv_secret_client_secret` | identity and secret names (never secrets) |
| | `p_authority_host`, `p_pbi_api_base`, `p_fabric_api_base`, `p_graph_api_base` | sovereign clouds |
| Scope | `p_only_workspaces`, `p_exclude_workspaces`, `p_source_capacity` | names or ids; empty = whole tenant; capacity name or id |
| | `p_include_personal_workspaces`, `p_include_inactive_workspaces` (`False`) | personal and non-active workspaces |
| Collection | `p_collect_gen1`, `p_collect_gen2` (`True`) | generations to harvest |
| | `p_collect_definitions` (`True`), `p_definition_source` (`admin`), `p_collect_gen2_definitions` (`False`) | deep inspection of definitions; which API reads Gen1 definitions first |
| | `p_collect_compute_engine`, `p_collect_refresh_history` (`True`) | enhanced compute setting, refresh history and schedules |
| | `p_collect_admin_datasources`, `p_verify_upstream_admin_api`, `p_collect_pipeline_references`, `p_collect_item_connections`, `p_enrich_owners_graph` (`False`) | optional collectors |
| Scanner | `p_scan_batch_size` (100), `p_scan_max_concurrent` (8, API max 16), `p_scan_poll_interval_sec` (10), `p_scan_timeout_sec` (900), `p_scan_lookback_days` (28) | incremental window 30 min to 30 days |
| | `p_scan_get_users` (`True`), `p_scan_dataset_expressions` (`False`) | extra Scanner flags |
| Throughput | `p_max_workers` (4), `p_rate_limit_scale` (0.9), `p_rate_limit_overrides` | parallelism and quota use; JSON such as `{"pbi_admin_dataflow_export": [[100, 3600]]}` |
| | `p_max_retries` (6), `p_retry_base_delay_sec`, `p_retry_max_delay_sec`, `p_max_retry_after_sec` (900) | retry policy; a longer `Retry-After` ends the phase |
| | `p_budget_definitions_per_run` (400), `p_budget_refresh_dataflows_per_run` (1500), `p_budget_datasources_per_run` (400) | per-run budgets, counted in **dataflows** (each costs one to three API calls) |
| | `p_refresh_history_ttl_hours` (24), `p_refresh_history_max_records` (100), `p_denied_retry_days` (7), `p_failed_retry_days` (1), `p_max_attempts_per_entity` (3), `p_batch_flush_size` (100) | incremental behaviour |
| Assessment | `p_stale_days` (90), `p_failing_consecutive` (5) | Deferred thresholds |
| | `p_wizard_available`, `p_preferred_path` (`auto`), `p_require_definition_for_ready` (`True`), `p_pilot_size` (10) | path and pilot policy |
| | `p_score_weights`, `p_environment_patterns` | JSON overrides: weights and bands, regexes for Dev / Test / Prod |
| | `p_governance_overrides_path`, `p_migration_results_table` (`dataflowmigrations`) | governance CSV, Save-As results table |
| Reporting | `p_export_reports`, `p_top_n` (25), `p_render_lineage_graph`, `p_lineage_graph_max_nodes` (60) | outputs; a table above 200 000 rows is written as CSV instead of an Excel sheet (the workbook keeps a note) |

## 4. Run modes

| Mode | What runs | Use it for |
|---|---|---|
| `incremental` | everything; Scanner reads only workspaces modified since the last complete scan **of the same scope and Scanner flags** (another scope or flag restarts a full, resumable scan), collectors skip up-to-date entities | scheduled runs |
| `full` | everything; full Scanner pass, every definition read again | first run, quarterly refresh of the baseline |
| `resume` | like `incremental`, continues `p_resume_run_id` (same log, same KPI row) | finishing a run that ended Partial |
| `refresh_only` | refresh history and schedules, then analysis, publish, report | daily health refresh during the programme |
| `analysis_only` | **no API call**: governance CSV, analysis, publish, report | tuning weights, rules, overrides, waves |

Recommended first run: `p_dry_run = True` with `p_only_workspaces` set to two or three workspaces, check the report, then switch `p_dry_run` off, then widen the scope and
run `full`. A run that stops on its deadline, budget or throttling is **Partial**: just run it again.

**What a Partial scan means.** The Scanner can fail a whole batch because of one workspace. The notebook retries a failed batch as two halves (up to 40 splits per run), so the bad
workspace ends up alone: it is marked `scan_state = Failed` in `Assessment_Workspaces`, logged in `Assessment_ErrorLog` (`entity_type` `ScanBatch`) and retried by every run, while its
batch-mates are scanned. An **active** workspace that is requested but missing from a successful result (deleted meanwhile, not yet visible) is `Failed` as well. While a workspace is
`Failed` the run stays Partial and the Scanner watermark does not advance, so nothing modified in the meantime is skipped. A workspace that is no longer active is legitimately
absent from the results: it is `NotScanned`, which is not a failure and does not hold the watermark back (it is requested again by every run while `p_include_inactive_workspaces` is on).
If a workspace keeps failing, exclude it with `p_exclude_workspaces` and open a support case with the scan id from `Assessment_ErrorLog`. `modifiedSince` honours
`p_include_personal_workspaces`; with `p_include_inactive_workspaces` the call asks for inactive workspaces explicitly.

**Changing the scope or the Scanner flags.** The watermark belongs to the scope settings and Scanner flags it was written for. Change one of them (a capacity, an include / exclude list,
`p_scan_get_users`, `p_scan_dataset_expressions`) and the next `incremental` run restarts a full scan, resumable like any other; change it back before the restart finished and it restarts again.
A workspace that drops out of the scope (moved to another capacity, excluded, deleted) is reset to `NotScanned`, so it is scanned as soon as it comes back, even if nobody modified it meanwhile.

## 5. Scale, throttling and scheduling

Quotas are per principal. The notebook spaces its calls so that it stays below them; the long pole of a first harvest is the **definition export**.

| Step | Calls | Quota | Consequence |
|---|---|---|---|
| Scanner | ceil(workspaces / 100) `getInfo` calls | 500/h, 16 concurrent scans | 20 000 workspaces = 200 calls: well within one run |
| `admin/groups`, `admin/dataflows`, `admin/items` | a handful of pages | 50/h, 200/h, 200/h | seconds |
| Gen1 definitions through the **admin export** | 1 per dataflow | **200/h** | 5 000 dataflows need about 25 hours of quota |
| Gen1 definitions through the **workspace API** (`p_definition_source = "workspace"`, identity is a member) | 1 per dataflow, workspaces that deny access are remembered and fall back to the admin export | much higher (default budget 3 000/h, adjustable) | the practical option for large tenants |
| Refresh history | 1 per dataflow (+1 schedule call per Gen2) | workspace APIs, Fabric job scheduler 200/min | 5 000 dataflows: well under an hour |

Practical plan for a large tenant:

1. Pipeline with a notebook activity, scheduled **hourly** for the first day. Each run takes what the quota allows (a wait longer than `p_max_retry_after_sec` ends the phase
   cleanly) and the checkpoints guarantee that no dataflow is read twice. Coverage is visible in `definition_status` (Collected / AccessDenied / NotAttempted).
2. Prefer `p_definition_source = "workspace"` when the service principal (or a service group) is a member of most workspaces.
3. Afterwards schedule **daily** (`incremental`): only changed workspaces and dataflows cost API calls. Add `refresh_only` runs during the migration programme.
4. Lower `p_rate_limit_scale` (for example 0.5) if other tools share the same service principal.
5. The pipeline can read the notebook exit value (`run_id`, `status`, counts) and set `p_fail_on_partial = True` to fail the activity when a run is not fully successful.

## 6. Governance overrides (CSV)

Optional input at `Files/dea/config/governance_overrides.csv` (UTF-8, BOM accepted); a template is in [`config/governance_overrides.sample.csv`](../config/governance_overrides.sample.csv).
A row with `dataflow_id` overrides that dataflow, a row with only `workspace_id` applies to every dataflow of the workspace (a dataflow row wins).

| Column | Values | Effect |
|---|---|---|
| `workspace_id`, `dataflow_id` | GUIDs | scope of the row (one of them is required) |
| `business_owner`, `technical_owner`, `support_team` | text | governance columns of `Assessment_DataflowSummary` |
| `business_criticality` | High / Medium / Low (also `Élevée`, `Moyenne`, `Faible`) | replaces the inferred criticality (`criticality_source = Override`) |
| `workspace_classification` | text | replaces the domain-based classification |
| `environment` | Dev / Test / Prod (also `recette`, `production` ...) | replaces deployment-pipeline and naming detection |
| `defer`, `defer_reason` | `true` / text | forces the class **Deferred** (backlog, wave 99) |
| `pin_wave` | 0-4 | pins a Ready or Needs Attention dataflow to a wave (other values are ignored: use `defer` for the backlog) |
| `wizard_status_actual` | Ready to migrate / Needs Attention / Upgrade unavailable (aliases accepted) | observed wizard status, for calibration |
| `rollback_plan_defined`, `uat_window_start`, `uat_window_end` | `true` / dates | governance checklist items |
| `notes`, `updated_by`, `updated_at` | text | free text |

Invalid values are ignored with a warning (listed in the log); the table `Assessment_GovernanceOverrides` is the merged result and can also be edited directly.
Environment detection order: governance override, deployment pipeline stage (first stage Dev, last Prod, others Test), naming convention (`p_environment_patterns` to
adapt the regexes), dataflow name, Unknown.

## 7. Operating the migration

1. **Assess.** Run `full`, review the KPI summary and the Excel workbook (`Files/dea/reports/<run>/`), fill the governance CSV for the critical workspaces, run `analysis_only`.
2. **Calibrate.** Run the Upgrade Wizard assessment (view only) on three or four representative workspaces, record `wizard_status_actual`, compare with `predicted_wizard_status`.
3. **Pilot (wave 0).** Migrate the pilot batch (one workspace at a time for the wizard). Validate: Gen2 refresh, lineage, downstream models and reports, durations and capacity
   consumption. Adjust the rules and weights if needed.
4. **Execute waves.** Work from `Assessment_MigrationBacklog`: filter `can_start_now`, group by `batch_id`.
   * *Upgrade Wizard* (id kept): remediate the `pre_migration_actions`, the **owner** runs the wizard on the workspace, refresh the dataflow, open and save every downstream
     linked dataflow (`Assessment_RebindWorklist`), perform the `post_migration_actions`.
   * *Save-As* (new id): use the Microsoft accelerator (see [05-accelerator-review.md](05-accelerator-review.md)), filtered on the dataflow ids of the batch; its results table is
     read by the next run (`p_migration_results_table`) and the Gen1 dataflows turn into `MigratedBySaveAs` with their `gen2_dataflow_id`; re-point the consumers listed in the
     rebind worklist, re-enable the schedules, recreate incremental refresh.
   * *Neither path fits* (Unsupported, wizard unavailable, Gen1 to be kept, linked tables across workspaces): see the [alternatives](03-assessment-framework.md#alternatives-to-the-upgrade-wizard)
     (Power Query template, OneLake shortcuts, Warehouse output).
   * *Linked dataflows*: Gen2 has no cascading refresh, and Gen1 only cascaded inside one workspace. Chain the refreshes of the migrated dataflows with a pipeline or staggered schedules
     ([details](03-assessment-framework.md#linked-dataflows-and-the-refresh-cascade)).
5. **Re-run** (`refresh_only` or `incremental`) after each batch: in-place upgrades appear as `Migrated` (type flip), the KPI row of the run feeds the burn-down, downstream
   dataflows whose upstream moved become startable.
6. **Retire.** After validation (and the soft-delete period of the wizard), remove the Gen1 dataflows listed as migrated.

## 8. Monitoring

| Where | What |
|---|---|
| Notebook output | structured log (`time | level | run id | phase | message`) and the final banner: status, per-phase duration, rows, API calls, retries, throttling, errors, per-bucket API statistics |
| `Assessment_RunLog` | one row per phase: status, duration, rows, API calls and retries, errors, message; the first row carries the effective parameters |
| `Assessment_ErrorLog` | endpoint, HTTP status, error code, truncated body, attempt, retryable |
| `Assessment_RunSummary` | counts by class, effort, API usage; trend and burn-down |
| `Files/dea/runs/<run>.json` | all of the above plus the access preflight, recent warnings and `persistence_problems` (what could not be stored); one file per attempt of a run (`<run>.json`, `<run>.attempt2.json`, ...) |
| Notebook exit value | `run_id`, `status` and counts for the calling pipeline |

Statuses: **Succeeded**; **Partial** (deadline, budget or throttling ended a phase early: re-run); **Failed** (a phase raised: see the error log). A critical failure (authentication,
access check) aborts the remaining steps, still persists the logs and fails the notebook. A step that works on the output of another is **Skipped** when that one did not complete
(`publish` after a failed `analysis`; `maintenance` and `report` after a failed or skipped `publish`): the message says why; fix the cause and run the section's cell again, the steps
that follow then run again too. The monitoring tables are stored independently of each other, so a
failing run-log write cannot hide the run summary; with `p_fail_on_partial = True` a run whose monitoring could not be stored fails too. Running the last cell again stores what is still
missing and nothing twice (rows already written are not appended again, the KPI row and the report of the attempt are rewritten). On `resume`, the KPI row accumulates the API
calls, retries, throttling and errors of all attempts.

## 9. Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `access_check` fails, `Denied (401/403)` in the preflight | tenant settings not enabled or service principal not in the security group; admin-consent Power BI permissions on the app | enable the settings, check the group, remove the permissions; the log prints a hint per API family |
| `workspaces`: "N workspaces reference a capacity that no capacity API returned" | the capacity list is incomplete for service principals | informational: those workspaces are kept as *Unresolved* (rule `CAP-002`) |
| Many dataflows `AccessDenied` for the definition | admin export denied and workspace access missing | add the service principal to the workspaces, or accept lower confidence (`DAT-001`) |
| `refresh_history_status = AccessDenied` for Gen1 | the transactions API requires workspace membership | add the identity to the workspaces |
| Gen2 `schedule_enabled` empty although the history is collected | the schedule call was denied (HTTP 401/403) | see `Assessment_ErrorLog` (`entity_type = DataflowSchedule`); the schedule is *unknown*, not "none"; check the identity's access to the item |
| Phases end **Partial** with "stopped early" | deadline, budget or a `Retry-After` longer than `p_max_retry_after_sec` | re-run; raise the budget or the deadline if the quota allows |
| `scan` Partial, "could not ingest scan batch" | unexpected Scanner payload | see `Assessment_ErrorLog`; the raw JSON is in `Files/dea/raw/<run>/scanner`; re-run |
| `scan` Partial, "N workspaces missing from the results" | the Scanner left out active workspaces (deleted meanwhile, not yet visible) | they are `scan_state = Failed` and retried by the next run; if one persists, check it in the portal or exclude it with `p_exclude_workspaces` |
| `scan` message "N split after a failure" | the Scanner failed a batch and the notebook retried it in halves | informational; a workspace still `Failed` at the end is the one the Scanner cannot process |
| `scan` Partial, "[AMBIGUOUS_REFERENCE] Reference `filePath` is ambiguous" | the Fabric / Delta MERGE engine rejected a write (seen on `Assessment_DataflowSources`) | the notebook retries with the low-shuffle merge disabled, then replaces the rows by key (delete, insert); the log says which. Re-run: the unscanned workspaces are retried |
| `definition_status = Failed`, "holds none of ..." or "returned no dataflow model" | the API answered without a definition | retried after `p_failed_retry_days`; nothing is stored as collected |
| "operation ... succeeded but its result is not available" | the long-running operation result expired or was not readable | re-run; the entity is retried |
| `publish` Failed with a row count mismatch | write problem on a Gold table | re-run `analysis_only`; check the Lakehouse (`maintenance` and `report` are skipped meanwhile) |
| `ConfigError` about `p_vacuum_retention_hours` or `p_files_root` at start | VACUUM retention below 168 hours, or a files folder outside `Files/` | fix the parameter |
| "attach a default Lakehouse" | the notebook has no default Lakehouse | attach it (or bind it with `--lakehouse-id`) |
| Readiness looks wrong for a dataflow | heuristic rule, missing definition | open `Assessment_UnsupportedFeatures` (evidence), compare with the wizard, tune the rule (impact, weight, `enabled`) in the rule catalog cell |
| Excel workbook missing | pandas or openpyxl not available | CSV files are written instead; install the libraries in the environment |

## 10. CI/CD of this repository

* **Source of truth**: `notebooks/src/*.py` (percent-format cells). `tools/build_notebook.py` builds the notebook and the generated documents;
  `python tools/build_notebook.py --check` fails when they are out of date (use it in the pull request build).
* **Tests**: `python -m pytest tests` (no Spark, no network). They cover the registry and generated DDL, the HTTP client, discovery, definitions, collectors, refresh,
  analysis, publication, reporting, orchestration (all run modes, resume, aborts) and execute **the generated `.ipynb` cell by cell** against a simulated tenant.
  `tests/test_spark_lake.py` is an opt-in Delta integration test (requires pyspark, delta-spark and a JDK 17+).
* **Promotion**: Fabric Git integration or a deployment pipeline moves the notebook between workspaces; per-environment parameters belong to the pipeline activity
  (`p_key_vault_url`, `p_lakehouse_schema`, scope). The notebook has no environment-specific content.
* **Versioning**: `NOTEBOOK_VERSION` (notebook) and `RULES_VERSION` (rule catalog) are written to every run (`Assessment_RunSummary`, `Assessment_RuleCatalog`, readiness rows).

## 11. Known limitations

* The Upgrade Wizard has no API: its verdict is **predicted** and must be calibrated (see [03-assessment-framework.md](03-assessment-framework.md#7-calibration)).
* The refresh schedule of a Gen1 dataflow comes from the Scanner only; Gen1 refresh history needs workspace membership; Gen2 `getDefinition` support for service principals is
  contradictory in the documentation (best effort); the `computeEngineBehavior` values are not documented (interpreted defensively).
* Row counts and data sizes are not exposed by the APIs; notebooks and external consumers are not part of the lineage.
* The analysis (graph closure, scoring, waves) runs on the driver in plain Python: it is sized for tens of thousands of dataflows. The transitive closure of a pathological hub graph costs
  more than linearly; it is bounded by the row budgets of `Assessment_DataflowDependencies` (2 000 per dataflow and direction, 500 000 in total) and the counts stay exact.
* A workspace that makes the Scanner fail is isolated, flagged `Failed` and retried by every run; it does not block the others, but it keeps the run Partial until it is excluded or fixed.
* The M analysis is lexical (no full M parser). Field selectors (`[Country]`), record keys and `let` steps are told apart from query references by their enclosing brackets, dotted names such as
  `Date.From` or `dim.Customer` are matched whole, and `let` scopes are modelled (a step shadows a query only inside its own `let`, and not in its own initializer, where the name still reads
  the query). Function parameters and the fields of a record literal are not modelled: a parameter or field that shares a name with a query can be counted as a reference. The definition
  and the Scanner remain the evidence.
* The notebook was developed and tested against a simulated tenant. It has **not been executed against a live tenant or on Spark/Delta in the authoring environment**:
  run it first with `p_dry_run = True` on a small scope, then run `tests/test_spark_lake.py` or a small real run to validate the Delta layer in your runtime.
