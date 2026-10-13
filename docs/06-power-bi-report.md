# Power BI report

A Direct Lake semantic model and a seven-page report over the Lakehouse tables written by the notebook. Everything is generated from the notebook schema registry by
[`tools/build_powerbi.py`](../tools/build_powerbi.py) into [`powerbi/`](../powerbi), as a Power BI project (PBIP: TMDL for the model, PBIR for the report), so it opens in Power BI Desktop
and goes through Git and deployment pipelines like any other project.

## Pages

| Page | Question it answers | Main visuals |
|---|---|---|
| **Overview** | How big is the estate and how ready is it? | dataflows, Gen1 / Gen2, ready share, effort, average complexity, **workspaces not scanned**; readiness, wave, path, complexity band, criticality; effort per workspace |
| **Readiness and blockers** | What stops the migration and how do I fix it? | findings by feature and severity, readiness by workspace, findings table with evidence, remediation and effort |
| **Migration plan** | In which order, by whom, how much work? | startable and blocked dataflows, effort per wave, batches (wave x workspace), the ordered work queue |
| **Dependencies and sources** | Who depends on whom, and on which sources and gateways? | related assets by type and direction, cross-workspace links and cycles, sources by category and connector, hosts and credential types |
| **Refresh health** | Which dataflows fail, and when? | runs per day by status, failed runs per dataflow, schedules, executions and error messages |
| **Estate and governance** | Which workspaces, capacities, owners, environments? | dataflows per capacity and owner, environments, workspace table with scan state |
| **Run monitor** | Did the notebook cover the tenant? | readiness trend over the runs, API calls and errors per run, run table |

The **Workspaces Not Scanned** card is the data-quality indicator: while it is not 0, some dataflows may be missing or incomplete (sources, gateways, lineage come from the Scanner).

## Model

| Table | Lakehouse table | Role |
|---|---|---|
| Dataflows | `assessment_dataflowsummary` | central table, one row per dataflow; most measures live here |
| Workspaces | `assessment_workspaces` | workspace dimension (including workspaces without dataflows) |
| Unsupported Features, Rules | `assessment_unsupportedfeatures`, `assessment_rulecatalog` | findings and the rule catalog |
| Migration Backlog, Wave Plan | `assessment_migrationbacklog`, `assessment_waveplan` | execution order and batches |
| Dependencies, Data Sources | `assessment_dataflowdependencies`, `assessment_dataflowsources` | lineage and connectivity |
| Refresh History, Run History | `assessment_refreshhistory`, `assessment_runsummary` | operations and monitoring |

Eight many-to-one relationships (on `dataflow_id`, `workspace_id` and `rule_id`), 62 explicit measures (`discourageImplicitMeasures`), keys and the numbers read through measures are hidden.
Direct Lake on OneLake: no import, no copy. Arrays (`tags`, `blocked_by`, `top_actions`, ...) are not supported by Direct Lake and are left out; the Excel workbook of the run keeps them.

## Use it

```powershell
# 1. the ids of the workspace and the Lakehouse the notebook writes to
python tools/build_powerbi.py --workspace-id <workspace guid> --lakehouse-id <lakehouse guid>

# 2a. open powerbi/DataflowAssessment.pbip in Power BI Desktop, or
# 2b. deploy the semantic model and the report to the workspace (az login first)
python tools/deploy_powerbi.py --workspace-id <workspace guid>
```

After each notebook run the Direct Lake model must be *framed* again to see the new data: it happens automatically when the model keeps its default "keep Direct Lake data up to date" setting,
otherwise refresh the semantic model (the notebook can be followed by a semantic model refresh activity in the pipeline). `python tools/build_powerbi.py --check` (and the test suite) fails
when the project is out of date with the registry.

## Verification

The model and the report were deployed and checked against a real run: all measures were evaluated with DAX queries and every visual's field combination was executed as a query; the
report files are validated against Microsoft's PBIR schemas. The rendering itself (colours, spacing) was not inspected: the export-to-image API is disabled on the tenant it was tested on.

## Limits

* The complexity band has no numeric order column, so its visuals are donuts (no wrong alphabetical ordering).
* Personal workspaces are listed in Workspaces; filter on `Is Personal` if they are not wanted.
* A new column in the registry is not added to the model automatically: add it to `SPEC` in the generator.
