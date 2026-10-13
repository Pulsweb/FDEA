# Lakehouse model

All tables are Delta tables in the default Lakehouse of the notebook (schema `dbo` by default, `p_lakehouse_schema = ""` for Lakehouses without schemas).
The notebook creates them (`CREATE TABLE IF NOT EXISTS`) and adds missing columns when the registry evolves. Everything below is generated from the
**schema registry** in the notebook: [`sql/lakehouse_ddl.sql`](../sql/lakehouse_ddl.sql) (DDL), [`docs/data-dictionary.md`](data-dictionary.md) (every column),
[`docs/rule-catalog.md`](rule-catalog.md) (rules).

## 1. Tables

| Layer | Table | Grain | Load | Keys | Z-order |
|---|---|---|---|---|---|
| Silver | `Assessment_Capacities` | capacity | MERGE, soft delete | `capacity_id` | - |
| Silver | `Assessment_Workspaces` | workspace | MERGE, soft delete | `workspace_id` | `workspace_id` |
| Silver | `Assessment_Dataflows` | dataflow (Gen1 and Gen2) | MERGE by phase-owned columns, soft delete | `dataflow_id` | `workspace_id`, `dataflow_id` |
| Silver | `Assessment_DataflowSources` | dataflow x data source | MERGE, soft delete | `dataflow_id`, `source_key` | - |
| Silver | `Assessment_Lineage` | lineage edge | MERGE, soft delete | `edge_id` | - |
| Silver | `Assessment_RefreshHistory` | refresh execution | MERGE, **partitioned by `start_month`** | `dataflow_id`, `refresh_id` | - |
| Gold | `Assessment_MigrationReadiness` | dataflow | snapshot | `dataflow_id` | `dataflow_id` |
| Gold | `Assessment_DataflowSummary` | dataflow, denormalised (the Excel master) | snapshot | `dataflow_id` | `workspace_id`, `dataflow_id` |
| Gold | `Assessment_UnsupportedFeatures` | dataflow x rule hit | snapshot | `dataflow_id`, `rule_id`, `entity_name` | - |
| Gold | `Assessment_DataflowDependencies` | dataflow x related asset (direct and transitive) | snapshot | `dataflow_id`, `direction`, `related_asset_type`, `related_asset_id`, `hop` | - |
| Gold | `Assessment_LineageNodes` | lineage node | snapshot | `node_id` | - |
| Gold | `Assessment_WavePlan` | wave x workspace batch | snapshot | `wave`, `workspace_id` | - |
| Gold | `Assessment_MigrationBacklog` | in-scope dataflow in execution order | snapshot | `dataflow_id` | `dataflow_id` |
| Gold | `Assessment_RebindWorklist` | dataflow x consumer to re-point | snapshot | `dataflow_id`, `consumer_id` | - |
| Gold | `Assessment_RunSummary` | run (one KPI row; a resumed run accumulates API usage and keeps its first start time) | MERGE by run id | `run_id` | - |
| Config | `Assessment_RuleCatalog` | rule | snapshot | `rule_id` | - |
| Config | `Assessment_GovernanceOverrides` | workspace or dataflow override | MERGE | `scope`, `workspace_id`, `dataflow_id` | - |
| Control | `Assessment_Checkpoint` | entity x phase | MERGE | `entity_type`, `entity_id`, `phase` | - |
| Control | `Assessment_RunLog` | run phase | append | `run_id`, `seq` | - |
| Control | `Assessment_ErrorLog` | error | append | `run_id`, `ts`, `entity_id`, `endpoint` | - |

The tables requested for the assessment map as follows: `Assessment_Workspaces`, `Assessment_Dataflows`, `Assessment_DataflowDependencies`,
`Assessment_DataflowSources`, `Assessment_RefreshHistory`, `Assessment_MigrationReadiness`, `Assessment_UnsupportedFeatures` and `Assessment_Lineage` exist with
exactly these names; the others support restartability, reporting and the hand-off to the migration tooling.

## 2. Conventions

* **Identifiers** are lowercase GUID strings (`gid()`), so joins never depend on casing. `dataflow_key` and `workspace_key` are deterministic signed 63-bit surrogate keys
  (blake2b of the GUID): stable across runs and engines, suitable as integer join keys in a semantic model.
* **Audit columns.** Current-state tables carry `first_seen_at`, `last_seen_at`, `is_deleted`, `deleted_at`, `run_id`. Snapshot and history tables carry `run_id` and
  `updated_at`. Nothing is physically deleted by the notebook: rows that disappear from the tenant are flagged (`is_deleted`) and revived when seen again. Deleting a workspace
  cascades: its dataflows, data sources and lineage edges (as source or target) are flagged with it, and `Assessment_RefreshHistory` keeps the history.
* **Types.** `STRING`, `BOOLEAN`, `INT`, `BIGINT`, `DOUBLE`, `TIMESTAMP` (naive UTC, session time zone forced to UTC), `DATE`, `ARRAY<STRING>`. Values are coerced
  before writing, so a bad API value becomes `NULL` instead of failing a batch.
* **Ownership of columns.** `Assessment_Dataflows` is written by several phases, each owning a column set (scan, definition, refresh, schedule, compute engine,
  source roll-up). A MERGE only updates the columns the phase supplies, so one phase can never null out another one's facts.
* **Schema evolution.** Missing registry columns are added with `ALTER TABLE ... ADD COLUMNS`. Gold overwrites use the table's physical column order, so evolved
  tables stay correct.
* **Natural keys.** Edges are keyed by a hash of source, target, relationship **and evidence**, so the same link seen by the Scanner and by the M parser is kept once per
  evidence and de-duplicated by the analysis.

## 3. Partitioning strategy

| Table | Strategy | Why |
|---|---|---|
| `Assessment_RefreshHistory` | `PARTITIONED BY (start_month)` (`yyyy-MM`) | append-only and the only table that grows without bound (about one row per refresh). MERGE statements are pruned to the months present in the batch, retention is a partition-pruned `DELETE`, month filters are cheap. |
| everything else | not partitioned | at most hundreds of thousands of rows: partitioning would create many tiny files and slow every query. Z-ORDER on the join keys gives the data skipping that matters. |

Reconsider only when a table exceeds roughly 50 million rows or 1 GB per partition. Retention example:

```sql
DELETE FROM dbo.Assessment_RefreshHistory WHERE start_month < '2026-01';
```

## 4. Performance recommendations

1. **Compact after every run.** `maintenance` runs `OPTIMIZE` (with `ZORDER BY` where defined) and reports what really ran: a table that fails to optimize makes the step **Partial**.
   Keep `p_run_maintenance = True`; add `VACUUM` with `p_vacuum_retention_hours >= 168` (the Delta default of 7 days: shorter values are rejected at start, 0 disables VACUUM).
   `p_files_root` must be a folder below `Files/` (no `..`, no drive): it is validated before anything is written.
2. **V-Order and optimized writes** are enabled by the storage layer (`spark.sql.parquet.vorder.default`, `spark.microsoft.delta.optimizeWrite.enabled`), the combination
   recommended for Direct Lake and the SQL analytics endpoint.
3. **Small compute is enough.** The workload is API-bound (quotas, not CPU): a Small or Medium pool is fine. Analysis runs on the driver in plain Python.
4. **Write less, write once.** Collectors flush micro-batches (`p_batch_flush_size`, default 100) instead of one write per dataflow; Gold tables are written once per run.
5. **Narrow MERGE keys.** All keys are natural identifiers (GUIDs, hashes); the null-safe join (`<=>`) tolerates missing parts without duplicating rows.
6. **Read Gold, not Silver.** Report from `Assessment_DataflowSummary`: it is denormalised (workspace, capacity, domain, environment, owner, readiness, scores, wave).
7. **Cap the closure.** `Assessment_DataflowDependencies` lists at most 2 000 related assets per dataflow and direction, nearest first (counts in
   `Assessment_MigrationReadiness` stay exact), within a global budget of 500 000 rows; once the budget is spent only direct (hop 1) rows are listed. A warning is logged when a hub dataflow
   is truncated.

Order-of-magnitude volumes for a tenant of 5 000 Gen1 dataflows in 20 000 workspaces (estimates, not measurements):

| Table | Rows |
|---|---|
| Workspaces / Capacities | 20 000 / 100 |
| Dataflows / DataflowSources | 5 500 / 25 000 |
| Lineage / LineageNodes | 60 000 / 30 000 |
| DataflowDependencies | 100 000 - 400 000 |
| UnsupportedFeatures | 25 000 |
| RefreshHistory | about 0.5 million after the first harvest (Gen1 keeps a limited history), then +1 row per refresh |

## 5. Semantic model recommendation (Direct Lake)

Build one Direct Lake semantic model on the Lakehouse (the Microsoft accelerator ships a similar model for its inventory; see
[05-accelerator-review.md](05-accelerator-review.md)).

| Table | Role | Notes |
|---|---|---|
| `Assessment_DataflowSummary` | main fact (grain: dataflow) | already contains workspace, capacity, domain, environment, owner, criticality, readiness, scores, wave |
| `Assessment_UnsupportedFeatures` | fact: findings per dataflow and rule | many-to-one to the summary; use for top blockers |
| `Assessment_RefreshHistory` | fact: refresh executions | many-to-one to the summary; month for time intelligence |
| `Assessment_DataflowDependencies` | fact: closure | many-to-one to the summary |
| `Assessment_WavePlan`, `Assessment_MigrationBacklog`, `Assessment_RebindWorklist` | detail tables | batches and work queue |
| `Assessment_RuleCatalog` | dimension | rule metadata |
| `Assessment_RunSummary` | trend fact | burn-down per run |
| `Assessment_Workspaces`, `Assessment_Capacities` | optional dimensions | contain soft-deleted rows: filter `is_deleted = false` |

Relationships (single direction, many to one): `UnsupportedFeatures[dataflow_id]`, `RefreshHistory[dataflow_id]`, `DataflowDependencies[dataflow_id]`,
`MigrationBacklog[dataflow_id]`, `RebindWorklist[dataflow_id]` -> `DataflowSummary[dataflow_id]`; `UnsupportedFeatures[rule_id]` -> `RuleCatalog[rule_id]`.
Array columns (`tags`, `top_actions`, `blocking_rules`, ...) are not usable in Direct Lake: keep them out of the model (the Excel export flattens them with `; `).

```dax
Dataflows        = DISTINCTCOUNT ( Assessment_DataflowSummary[dataflow_id] )
In scope         = CALCULATE ( [Dataflows], NOT ISBLANK ( Assessment_DataflowSummary[readiness] ) )
Ready %          = DIVIDE ( CALCULATE ( [Dataflows], Assessment_DataflowSummary[readiness] = "Ready" ), [In scope] )
Effort (days)    = SUM ( Assessment_DataflowSummary[effort_days] )
Avg complexity   = AVERAGE ( Assessment_DataflowSummary[complexity_score] )
Migrated         = CALCULATE ( [Dataflows], Assessment_DataflowSummary[migration_status] IN { "Migrated", "MigratedBySaveAs" } )
Progress %       = DIVIDE ( [Migrated], [Migrated] + [In scope] )
```

## 6. Example queries

```sql
-- Workload per wave
SELECT wave, wave_label, readiness, COUNT(*) AS dataflows, ROUND(SUM(effort_days), 1) AS effort_days
FROM dbo.Assessment_MigrationReadiness
WHERE readiness IS NOT NULL
GROUP BY wave, wave_label, readiness
ORDER BY wave, readiness;

-- Most frequent blockers and attention points
SELECT rule_id, feature_name, severity, COUNT(DISTINCT dataflow_id) AS dataflows
FROM dbo.Assessment_UnsupportedFeatures
WHERE impact_wizard IN ('Unsupported', 'NeedsAttention') OR impact_saveas IN ('Unsupported', 'NeedsAttention')
GROUP BY rule_id, feature_name, severity
ORDER BY dataflows DESC;

-- What breaks if this dataflow changes? (replace the id)
SELECT related_asset_type, related_asset_name, related_workspace_name, hop, path
FROM dbo.Assessment_DataflowDependencies
WHERE dataflow_id = '00000000-0000-0000-0000-000000000000' AND direction = 'DOWNSTREAM'
ORDER BY hop, related_asset_type;

-- Gateway dependencies
SELECT gateway_id, gateway_name, datasource_type, COUNT(DISTINCT dataflow_id) AS dataflows
FROM dbo.Assessment_DataflowSources
WHERE uses_gateway = true AND (is_deleted = false OR is_deleted IS NULL)
GROUP BY gateway_id, gateway_name, datasource_type
ORDER BY dataflows DESC;

-- Work queue for the next batch: what can start now, per workspace (the Upgrade Wizard works per workspace)
SELECT batch_id, workspace_name, dataflow_name, owner_upn, recommended_path, effort_days
FROM dbo.Assessment_MigrationBacklog
WHERE can_start_now
ORDER BY seq;

-- Burn-down
SELECT started_at, ready_count, needs_attention_count, unsupported_count, deferred_count, migrated_count, total_effort_days
FROM dbo.Assessment_RunSummary
ORDER BY started_at;

-- Refresh failures of the last 30 days
SELECT d.workspace_name, d.dataflow_name, h.start_time, h.error_code, h.error_message
FROM dbo.Assessment_RefreshHistory h
JOIN dbo.Assessment_DataflowSummary d ON d.dataflow_id = h.dataflow_id
WHERE h.status_normalized = 'Failed' AND h.start_date >= date_sub(current_date(), 30)
ORDER BY h.start_time DESC;
```

## 7. Data model recommendations

* Treat the Gold tables as **disposable**: they are rebuilt from Silver on every run (and by `analysis_only`). Never edit them by hand; put manual input in the governance CSV.
* Keep manual knowledge in `Assessment_GovernanceOverrides` (business owner, criticality, support team, environment, defer, pinned wave, observed wizard status,
  rollback plan, acceptance window). It survives every re-run and feeds the analysis.
* Store the run history (`Assessment_RunSummary`) for at least the duration of the programme: it is the burn-down chart.
* Keep `Files/dea/raw` for 30 days (`p_raw_retention_days`) so a rule change can be validated against the same Scanner payloads.
* Add columns through the registry (`notebooks/src/01_a_registry.py`), then run `python tools/build_notebook.py`; the DDL and the dictionary regenerate and existing
  tables evolve on the next run.
