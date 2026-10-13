# Fabric Dataflow Estate Assessment (FDEA)

Tenant-wide assessment of **Power BI Dataflow Gen1 and Fabric Dataflow Gen2** for a migration programme towards Dataflow Gen2. A Fabric notebook harvests metadata with the
Power BI / Fabric REST APIs (service principal, Scanner API first), persists a dimensional model into **Delta tables of a Lakehouse**, and scores every dataflow:
**readiness class** (Ready / Needs Attention / Unsupported / Deferred), **complexity score**, **effort**, **recommended path** (Upgrade Wizard or Save-As) and **migration wave**.

* Restartable and incremental: per-entity checkpoints, watermarked Scanner scans (the watermark only advances after a complete scan of the same scope), API budgets, graceful stop on deadline or throttling.
* Throttling-aware: documented quotas modelled as rate-limit buckets, `429` / `Retry-After` handling, pagination, long-running operations.
* Defensive: a Scanner batch that fails is split to isolate the one bad workspace; empty or partial API answers are recorded as failures, never as collected data; a failed analysis never reaches the Gold tables or the report; deleted workspaces take their dataflows and lineage with them.
* Explainable: every verdict comes from a [rule catalog](docs/rule-catalog.md) with evidence, remediation and effort.
* Reusable: one notebook, one parameters cell, a generated schema ([DDL](sql/lakehouse_ddl.sql), [dictionary](docs/data-dictionary.md)), a test suite that also runs the built `.ipynb`.

## Documentation

| Document | Content |
|---|---|
| [docs/01-architecture.md](docs/01-architecture.md) | architecture diagram, notebook structure, collection workflow, API usage, incremental design, throttling, storage, metadata model, security, reuse of the existing assets |
| [docs/02-lakehouse-model.md](docs/02-lakehouse-model.md) | tables, keys, partitioning, performance, volumes, Direct Lake model, example queries |
| [docs/03-assessment-framework.md](docs/03-assessment-framework.md) | checklist, readiness matrix, scoring algorithm, effort, wave logic, recommendations, calibration |
| [docs/04-runbook.md](docs/04-runbook.md) | prerequisites, service principal, parameters, run modes, scale and scheduling, governance CSV, operating the migration, troubleshooting, CI/CD |
| [docs/05-accelerator-review.md](docs/05-accelerator-review.md) | review of the Microsoft DFG2 migration accelerator and how both solutions work together |
| [docs/06-power-bi-report.md](docs/06-power-bi-report.md) | the Direct Lake semantic model and the seven-page Power BI report over the results, how to generate, open and deploy them |
| [docs/data-dictionary.md](docs/data-dictionary.md), [docs/rule-catalog.md](docs/rule-catalog.md), [sql/lakehouse_ddl.sql](sql/lakehouse_ddl.sql) | generated from the notebook registry |

## Quick start

1. Prepare the identity and tenant settings, and Key Vault secrets ([runbook 1](docs/04-runbook.md#1-prerequisites)).
2. Import [`notebooks/Dataflow_Assessment.ipynb`](notebooks/Dataflow_Assessment.ipynb) into a Fabric workspace and attach a Lakehouse (schema-enabled, `dbo`).
3. Set `p_key_vault_url` (or `p_auth_mode = "user"`) in the first cell. First run: `p_dry_run = True` and `p_only_workspaces = "<a few workspaces>"`.
4. Run all cells. Results: Delta tables `Assessment_*`, reports under `Files/dea/reports/<run id>/` (summary, Excel workbook, lineage graph), run report under `Files/dea/runs/`.
5. Switch `p_dry_run` off, widen the scope, run `full`, then schedule `incremental` (hourly for the first day on a large tenant, then daily).
6. Optionally provide [`config/governance_overrides.sample.csv`](config/governance_overrides.sample.csv) as `Files/dea/config/governance_overrides.csv` (business owner, criticality, defer, pin a wave, ...).

## What is collected

| Requirement | Where |
|---|---|
| General: workspace, capacity, dataflow name / id / type, owner, modification date, description, tags | `Assessment_DataflowSummary` (denormalised), `Assessment_Dataflows`, `Assessment_Workspaces`, `Assessment_Capacities`. Creation date is filled when an API exposes it (Gen1 payloads do not) |
| Operational: last refresh, status, duration, schedule, history, errors | `Assessment_Dataflows` (`last_refresh_*`, `refresh_*`, `schedule_*`, `consecutive_failures`), `Assessment_RefreshHistory` |
| Connectivity: sources, types, gateway usage, credential type, parameters, linked / referenced dataflows | `Assessment_DataflowSources` (credential type where accessible), `Assessment_Lineage`, `parameter_count`, `linked_entity_count` |
| Technical complexity: queries, entities, custom functions, parameters, incremental refresh, computed and linked entities, unsupported features, score | `Assessment_Dataflows` (counts and `definition_findings`), `Assessment_UnsupportedFeatures`, `Assessment_MigrationReadiness` (`complexity_score`, components) |
| Dependencies: upstream / downstream, models, reports, dashboards, other assets, lineage | `Assessment_DataflowDependencies`, `Assessment_Lineage`, `Assessment_LineageNodes`, `Assessment_RebindWorklist` |
| Governance: owner, support team, classification, criticality, environment | `Assessment_DataflowSummary`, `Assessment_Workspaces`, `Assessment_GovernanceOverrides` |
| Readiness, recommendation, waves | `Assessment_MigrationReadiness`, `Assessment_WavePlan`, `Assessment_MigrationBacklog` |

## Repository layout

```text
notebooks/
  Dataflow_Assessment.ipynb     the notebook (generated, do not edit)
  src/                          percent-format sources, one or two files per section (source of truth)
sql/lakehouse_ddl.sql           CREATE TABLE statements (generated)
powerbi/                        Power BI project: Direct Lake semantic model + report (generated by tools/build_powerbi.py)
docs/                           documentation (data dictionary and rule catalog generated)
config/                         governance overrides template
tools/build_notebook.py         builds the notebook and the generated documents (--check for CI)
tools/build_powerbi.py          builds the Power BI project from the registry; tools/deploy_powerbi.py deploys it
tests/                          offline test suite (simulated Power BI / Fabric / Graph tenant)
checklist.md, *.ipynb, *.txt    the original project material (kept as is)
```

Edit `notebooks/src/*.py`, then run `python tools/build_notebook.py`. Install the test dependencies with `pip install -r requirements-dev.txt`, run the tests with `python -m pytest tests`.

## Status and honesty

* **Tested offline.** The suite (600+ tests) covers the registry, HTTP client (retry, 429, pagination, long-running operations), discovery, definition analysis (M scopes checked against generated expressions), collectors, refresh
  history, the analysis engine (rules, scores, graph algorithms, waves, randomised invariants), publication, reporting, orchestration (every run mode, resume, abort, dry run) and it **executes the built
  `.ipynb` cell by cell** against a simulated tenant.
* **Not run against a live tenant**, and the Delta layer has been exercised with a recording test double only (no Spark in the authoring environment: Windows without winutils).
  `tests/test_spark_lake.py` is a ready-made Spark / Delta integration test (pyspark, delta-spark, JDK 17+). Start with `p_dry_run = True` on a small scope.
* The Upgrade Wizard has no API: its verdict is **predicted** and should be calibrated ([framework 7](docs/03-assessment-framework.md#7-calibration)).
* Some collectors are best effort by design (Gen1 refresh history needs workspace membership; Gen2 `getDefinition` for service principals is documented contradictorily).

## How it was built

Endpoints, scopes, quotas and payloads were verified against the Microsoft documentation through the Fabric MCP documentation tools (item, admin and platform API
specifications) and research of the Power BI admin / Scanner documentation; the Fabric skills of the environment (`dataflows-cli`, `spark-cli`, `e2e-medallion-architecture`,
`onelake-catalog-govern-cli`, `search-consumption-cli`) provided the Gen2 definition formats, the Spark notebook conventions, the medallion layering and the governance data
sources; the GitHub MCP server was used to review the Microsoft accelerator at the pinned commit. Nothing was created or modified in a Fabric workspace.
