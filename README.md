<h1 align="center">Fabric Dataflow Estate Assessment (FDEA)</h1>

<p align="center">
  <strong>FDEA is a Microsoft Fabric notebook that inventories every Power BI Dataflow Gen1 and Fabric Dataflow Gen2
  of a tenant and turns it into a migration plan towards Dataflow Gen2: readiness, complexity, effort, path and wave
  for each dataflow.</strong>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
</p>

> [!CAUTION]
> This is a community reference implementation. It is **not an official Microsoft product**, is not supported by Microsoft, and there is a risk that things might break as Microsoft APIs evolve. See the [disclaimer](#license-and-disclaimer).

**Contents:**
[Why FDEA](#why-fdea) ·
[Get started](#get-started) ·
[How it works](#how-it-works) ·
[What is assessed](#what-is-assessed) ·
[Related solutions](#related-solutions) ·
[Limitations](#limitations) ·
[Security and privacy](#security-and-privacy) ·
[Documentation](#documentation) ·
[Credits](#credits) ·
[License and disclaimer](#license-and-disclaimer)

## Why FDEA

Migrating a large estate of dataflows to Gen2 raises the same questions in every tenant: which dataflows exist, who owns them, what do they depend on, which ones are ready, which ones need work, and in which order should they move? The answers are scattered across several admin and workspace APIs, and the APIs are throttled.

FDEA started from a real customer scenario with hundreds of Gen1 dataflows. The approach is generic, so it has been generalized and shared as community IP. Adapt the rules, scoring and reports to your own needs, and feedback is welcome through GitHub issues.

* **One inventory for the whole tenant.** Gen1 and Gen2 dataflows with owner, capacity, schedule, refresh history, data sources, gateways and lineage to downstream models and reports.
* **Explainable verdicts.** Every finding comes from a [catalog of 29 rules](docs/rule-catalog.md) with evidence, remediation and effort, for both the Upgrade Wizard and Save-As.
* **A plan, not just a list.** Readiness class, 0-100 complexity score, effort in person-days, recommended path, dependency-ordered waves and a worklist of consumers to re-point.
* **Safe to run on a large tenant.** Checkpoints, incremental Scanner scans, API budgets and `429` handling: a run that stops early is simply run again.
* **Read-only.** The notebook writes nothing to your tenant, only to the Lakehouse attached to it.
* **One notebook.** No local tooling: import it into Fabric and run it.

## Get started

You deploy FDEA by importing one notebook into a Fabric workspace. The [runbook](docs/04-runbook.md) details each step and every parameter.

### 1. Prepare an identity

Use a **service principal** (recommended) with its tenant id, client id and secret stored in **Azure Key Vault**, or run the notebook as a **Fabric administrator** (`p_auth_mode = "user"`). For a service principal, enable *Service principals can access read-only admin APIs* and *Service principals can call Fabric public APIs* for a security group containing it, and do **not** grant admin-consent Power BI API permissions to the app.
[Details](docs/04-runbook.md#service-principal-and-tenant-settings)

### 2. Import the notebook

Download [`notebooks/Dataflow_Assessment.ipynb`](notebooks/Dataflow_Assessment.ipynb), import it into a Fabric workspace, and attach a **Lakehouse** as its default Lakehouse (schema-enabled recommended).

### 3. Set the identity

In the first cell, set `p_key_vault_url = "https://<your-vault>.vault.azure.net/"`. The secret names default to `dea-tenant-id`, `dea-client-id` and `dea-client-secret`. Skip this when using `p_auth_mode = "user"`.

### 4. Try it on a small scope

Set `p_dry_run = True` and `p_only_workspaces = "<workspace name or id>"`, then select **Run all**. Nothing is written to the Lakehouse.

### 5. Run it for real

Set `p_dry_run = False`, clear `p_only_workspaces`, and run with `p_run_mode = "full"`. Then schedule `incremental` runs ([run modes and scheduling](docs/04-runbook.md#4-run-modes)).

Results are in the Lakehouse tables `Assessment_*` (start with `Assessment_DataflowSummary`) and in `Files/dea/reports/<run id>/` (summary, Excel workbook, lineage graph). The last cell prints the status of the run: `Succeeded`, `Partial` (run it again) or `Failed` (see `Assessment_ErrorLog` and the [troubleshooting table](docs/04-runbook.md#9-troubleshooting)).

For the optional Power BI report, see [Power BI report](#power-bi-report).

## How it works

The notebook reads the Power BI and Fabric APIs, builds the inventory in Delta tables, and runs the analysis on top of it. The Scanner API is used first, because one call describes up to 100 workspaces; per-item APIs are only used for what it cannot tell.

```mermaid
flowchart LR
    KV["Azure Key Vault<br/>service principal secrets"] --> NB
    API["Power BI Admin + Scanner API<br/>Fabric APIs (read only)"] --> NB
    NB["Fabric notebook<br/>Dataflow_Assessment"] --> LH[("Lakehouse<br/>Assessment_* tables<br/>Files/dea")]
    LH --> PBI["Direct Lake model<br/>and Power BI report"]
    LH --> XL["Excel workbook<br/>and lineage graph"]
```

| Component | What it does |
|---|---|
| Notebook ([notebooks/Dataflow_Assessment.ipynb](notebooks/Dataflow_Assessment.ipynb)) | Ten sections: configuration, authentication, workspace and dataflow discovery, definitions and dependencies, refresh history, readiness analysis, Delta persistence, reporting, monitoring. Every setting is a `p_` parameter in the first cell |
| Lakehouse tables ([sql/lakehouse_ddl.sql](sql/lakehouse_ddl.sql)) | 20 `Assessment_*` tables: inventory, lineage, findings, readiness, wave plan, backlog, rebind worklist and run monitoring. Described in the [data dictionary](docs/data-dictionary.md) |
| Assessment framework | Rules, complexity score, effort and waves, described in the [assessment framework](docs/03-assessment-framework.md). Weights and thresholds can be tuned and re-applied with `p_run_mode = "analysis_only"`, without any API call |
| Power BI project ([powerbi/](powerbi)) | A Direct Lake semantic model and a seven-page report over the tables |

More in the [architecture](docs/01-architecture.md).

### Power BI report

Pages: *Overview*, *Readiness and blockers*, *Migration plan*, *Dependencies and sources*, *Refresh health*, *Estate and governance* and *Run monitor*. Before opening `powerbi/DataflowAssessment.pbip` in Power BI Desktop, edit [`expressions.tmdl`](powerbi/DataflowAssessment.SemanticModel/definition/expressions.tmdl) and replace the workspace and Lakehouse ids in the OneLake URL with yours. Details: [docs/06-power-bi-report.md](docs/06-power-bi-report.md).

## What is assessed

| | |
|---|---|
| **Supported** | Power BI **Dataflow Gen1** and Fabric **Dataflow Gen2** items, as inventory. The migration verdicts target a Gen1 to Gen2 migration |
| **Not in scope** | Dataflow Gen2 CI/CD specifics, and GCC tenants (the Upgrade Wizard is not available there) |
| **Not assessed** | Data volumes and row counts (no API), and consumers that are not Power BI or Fabric items known to the Scanner: notebooks, external applications, Excel files, Power Automate flows |

| Collected | Where |
|---|---|
| Workspace, capacity, owner, type, dates, tags | `Assessment_DataflowSummary`, `Assessment_Dataflows`, `Assessment_Workspaces`, `Assessment_Capacities` |
| Last refresh, status, duration, schedule, errors | `Assessment_Dataflows`, `Assessment_RefreshHistory` |
| Data sources, gateways, credential types | `Assessment_DataflowSources` |
| Queries, entities, functions, parameters, incremental refresh, unsupported features | `Assessment_Dataflows`, `Assessment_UnsupportedFeatures` |
| Upstream and downstream dependencies, lineage | `Assessment_DataflowDependencies`, `Assessment_Lineage`, `Assessment_LineageNodes`, `Assessment_RebindWorklist` |
| Readiness, recommendation, waves | `Assessment_MigrationReadiness`, `Assessment_WavePlan`, `Assessment_MigrationBacklog` |

An optional governance CSV (`Files/dea/config/governance_overrides.csv`) adds business owner, criticality, environment, deferrals and pinned waves ([format](docs/04-runbook.md#6-governance-overrides-csv)).

## Related solutions

FDEA decides **what** to migrate, **how** and **when**. Microsoft's [DFG2 migration accelerator](https://github.com/microsoft/fabric-toolbox/tree/main/accelerators/DFG2-migration-accelerator), in the Fabric Toolbox, **executes** the Save-As migration. They are complementary and share the same Lakehouse conventions.

| | DFG2 migration accelerator | **FDEA** (this repository) |
|---|---|---|
| **Role** | Inventory, Save-As conversion and re-pointing of downstream items | Assessment, scoring, wave planning and rebind worklist |
| **Decision support** | None: no readiness, complexity, effort or waves | Readiness class, complexity score, effort, recommended path, waves |
| **Throttling and restart** | Plain API calls, full re-inventory on each run | Rate-limit buckets, checkpoints, incremental scans |
| **Data model** | Tables overwritten at each run, columns stored as strings | Typed tables with history and soft deletes |
| **Status** | Solution accelerator maintained in `microsoft/fabric-toolbox` | Community project |

After a Save-As, the results table of the accelerator (`p_migration_results_table`, default `dataflowmigrations`) is read by the next FDEA run, so migrated dataflows drop out of the plan. The review, based on a specific version of the accelerator, is in [docs/05-accelerator-review.md](docs/05-accelerator-review.md).

## Limitations

* **The Upgrade Wizard has no API.** Its verdict is predicted by the rules and should be calibrated on a sample of workspaces ([calibration](docs/03-assessment-framework.md#7-calibration)).
* **Some data needs workspace membership.** Gen1 refresh history, and definitions read through the workspace API, need the identity to be a workspace member. Without it, FDEA records `AccessDenied` and lowers the confidence of the verdict instead of failing.
* **Some behaviours are not documented by Microsoft.** Gen2 `getDefinition` for service principals is documented inconsistently (off by default), and `computeEngineBehavior` values are interpreted defensively. Rules marked `Heuristic` in the [rule catalog](docs/rule-catalog.md) infer limitations from indirect signals, and the M analysis is lexical, not a full parser.
* **Scale.** The analysis runs on the Spark driver in plain Python and is sized for tens of thousands of dataflows.
* **Throttling.** On a large tenant the first harvest can take several runs: the Gen1 definition export through the admin API is capped at 200 requests per hour ([scale and scheduling](docs/04-runbook.md#5-scale-throttling-and-scheduling)).
* **Manual steps remain:** the tenant settings, the identity and the Power BI model's workspace and Lakehouse ids.

The full list is in the [runbook](docs/04-runbook.md#11-known-limitations).

## Security and privacy

* **What is stored.** Workspace, dataflow and data source names, owner and admin user principal names, and the definitions-derived facts used by the rules. Raw Scanner JSON is kept in `Files/dea/raw` for `p_raw_retention_days` days (30 by default).
* **Where it is stored.** Only in the Lakehouse you attach to the notebook. Restrict access to it (workspace roles, OneLake security) and protect the governance CSV.
* **Secrets.** Only Key Vault secret *names* are parameters. Secrets are read with `notebookutils.credentials.getSecret`, tokens stay in memory and are not logged, and error bodies are truncated before they are stored.
* **Permissions.** Read-only admin APIs. Workspace membership and Microsoft Graph `User.Read.All` are optional and only needed by the collectors that use them. Nothing in the notebook modifies the tenant.
* **Your responsibility.** You run FDEA in your own tenant. Securing the workspace and the identity, and complying with your organization's data policies, remain your responsibility.

## Documentation

| Document | Content |
|---|---|
| [01-architecture.md](docs/01-architecture.md) | Architecture, API usage, incremental design, security |
| [02-lakehouse-model.md](docs/02-lakehouse-model.md) | Tables, Direct Lake model, example queries |
| [03-assessment-framework.md](docs/03-assessment-framework.md) | Readiness matrix, scoring, effort, waves |
| [04-runbook.md](docs/04-runbook.md) | Prerequisites, parameters, run modes, scheduling, troubleshooting |
| [05-accelerator-review.md](docs/05-accelerator-review.md) | Relationship with the Microsoft DFG2 accelerator |
| [06-power-bi-report.md](docs/06-power-bi-report.md) | Semantic model and report |
| [data-dictionary.md](docs/data-dictionary.md), [rule-catalog.md](docs/rule-catalog.md) | Generated reference |

## Credits

* **The Microsoft [DFG2 migration accelerator](https://github.com/microsoft/fabric-toolbox/tree/main/accelerators/DFG2-migration-accelerator)** in the [Fabric Toolbox](https://github.com/microsoft/fabric-toolbox). FDEA adopted its Scanner-first discovery with the real payload fields, its Save-As semantics and its results table, as described in the [review](docs/05-accelerator-review.md).
* **Microsoft documentation** for the Power BI REST and Scanner APIs, the Fabric REST APIs and Dataflow Gen2.

## License and disclaimer

FDEA is released under the [MIT License](LICENSE).

FDEA is a community reference implementation. It is **not an official Microsoft product**, is not affiliated with or endorsed by Microsoft, and comes with no official support. It does not replace official Microsoft documentation, guidance or support. It is provided "as is", without warranties or guarantees. You are responsible for validating it against your own security, compliance, governance, performance and production requirements before use. Microsoft product behavior and APIs evolve: refer to the official Microsoft documentation for authoritative information.

Microsoft, Power BI, Microsoft Fabric and Azure are trademarks of the Microsoft group of companies.
