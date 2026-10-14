# Assessment framework

The assessment turns the harvested facts into four things per Gen1 dataflow: a **readiness class**, a **complexity score** with an **effort estimate**, a **recommended
migration path** and a **wave**. Everything is deterministic, driven by the [rule catalog](rule-catalog.md), and explained: every verdict carries its reason, evidence and
remediation. Gen2 dataflows are out of scope (their readiness is `NULL`); dataflows already copied with Save-As are tracked but no longer planned.

## 1. Assessment checklist

The French workshop checklist (`checklist.md`) is automated wherever the platform exposes the information. Controls that need a human stay explicit: they are inputs of
the [governance overrides](04-runbook.md#6-governance-overrides-csv) or manual checks.

| Checklist control | Answered by | Automation |
|---|---|---|
| **General information** - name, id, workspace, capacity, technical owner | `dataflow_name`, `dataflow_id`, `workspace_name`, `capacity_name` / `capacity_sku`, `owner_upn` | automated |
| business owner, business criticality | governance CSV (`business_owner`, `business_criticality`); criticality is **inferred** otherwise (`criticality_source`) from environment, endorsement, consumers, schedule frequency | input + inferred |
| **1. Eligibility** - appears in the Upgrade Wizard, status Ready / Needs Attention / Not Supported | `status_wizard`, `predicted_wizard_status` (`Ready to migrate`, `Needs Attention`, `Upgrade unavailable`); the observed status is recorded in `wizard_status_actual` to calibrate the rules | predicted, then confirmed manually |
| remediation actions documented | `top_actions`, `Assessment_UnsupportedFeatures.remediation` | automated |
| **2. Data sources** - inventory, type, gateway required | `Assessment_DataflowSources` (`datasource_type`, `source_category`, `server`, `database`, `url`, `uses_gateway`, `gateway_name`, `is_on_premises`, `credential_type` when accessible) | automated |
| sources still active, accesses and credentials, gateways operational | `is_misconfigured` (rule `CON-001`), gateway rule `GW-001` (supported version, OAuth2 over gateway), `credential_type`; service accounts are identified by the owner review | partly automated |
| **3. Functional analysis** - parameters, custom functions, dependencies, documented transformations, critical business logic | `parameter_count`, `custom_function_count`, `query_count`, `entity_count`, `native_query_count`, `computed_entity_count`, `m_script_chars`, `definition_findings` (JSON evidence); documentation and critical logic are reviewed with the owner | facts automated, review manual |
| **4. Upstream dependencies** | `Assessment_DataflowDependencies` (`direction = UPSTREAM`), `upstream_dataflow_count`, rules `LNK-001`, `LNK-004`, `SEQ-001` | automated |
| **5. Downstream consumers** - semantic models, reports, other dataflows, pipelines | `Assessment_DataflowDependencies` (`DOWNSTREAM`), `downstream_*_count`, `Assessment_RebindWorklist`, rules `LNK-002`, `LNK-003`, `DQ-001`, `PIP-001`, `REB-001` | automated; **notebooks and external consumers are not exposed by any API** (check manually) |
| **6. Refresh** - last refresh succeeded, error history, frequency, duration | `last_refresh_status`, `refresh_ok`, `consecutive_failures`, `last_error_message`, `Assessment_RefreshHistory`, `schedule_enabled`, `schedule_runs_per_day`, `schedule_times`, `refresh_avg_duration_sec`, `refresh_p95_duration_sec`, `last_success_at` | automated (Gen1 history needs workspace membership) |
| migration window | derived from `schedule_times` / `schedule_timezone`; the window itself is agreed manually | manual |
| baseline: rows produced, data size | not exposed by the APIs | manual |
| **7. Governance** - owner identified, business validator, rollback strategy, acceptance window | `owner_upn`, `owner_enabled` (rule `OWN-001`), governance CSV (`business_owner`, `rollback_plan_defined`, `uat_window_start`, `uat_window_end`) | input |
| **8. Post-migration validation** - migration succeeded, Gen2 refresh succeeded, lineage check | re-run the notebook: an in-place upgrade shows as `migration_status = Migrated` (type flip), a Save-As as `MigratedBySaveAs` with `gen2_dataflow_id`; the Gen2 row carries its own refresh status; `Assessment_Lineage` shows the new bindings | automated tracking |
| data, semantic model, report and business validation | outside the scope of the harvest; use `Assessment_RebindWorklist` as the validation list | manual |
| **Final decision** - classification | `readiness`, `recommended_path` (see the mapping below) | automated |
| Go / No Go | `Assessment_MigrationBacklog.can_start_now`, `pre_migration_actions`; the decision stays human | manual |

Classification mapping: *Ready for Upgrade* = `Ready`; *Needs Attention* = `Needs Attention`; *Not Supported* = `Unsupported`; *Manual migration required* =
`recommended_path = ManualRebuild`; *Deferred* = `Deferred`.

## 2. Readiness matrix

### Classes

| Class | Meaning | Typical next step |
|---|---|---|
| **Ready** | no blocking and no attention finding on the recommended path | migrate in its wave |
| **Needs Attention** | migratable once the listed findings are handled (before or after the migration) | remediate, then migrate |
| **Unsupported** | blocked on every available path | fix the blocker and re-assess (`FixThenReassess`) or rebuild as a native Gen2 (`ManualRebuild`) |
| **Deferred** | not now: stale and unused, failing refresh, blocked by an upstream, or deferred by governance | retire, fix first, or wait |

### Evaluation order

| # | Condition | Class | Path |
|---|---|---|---|
| 1 | governance override `defer` | Deferred | `None` |
| 2 | no successful refresh for `p_stale_days` (90) **and** no downstream consumer (`STA-001`) | Deferred | `Retire` |
| 3 | `Unsupported` on the recommended path | Unsupported | `FixThenReassess` (connectivity, governance, capacity or data-quality blockers) or `ManualRebuild` |
| 4 | `p_failing_consecutive` (5) consecutive failed refreshes (`REF-001`) | Deferred | `FixThenReassess` |
| 5 | an upstream dataflow is Unsupported or Deferred, transitively, cycles included (`SEQ-001`) | Deferred | `FixThenReassess` |
| 6 | `NeedsAttention` on the recommended path | Needs Attention | `UpgradeWizard` or `SaveAs` |
| 7 | otherwise | Ready | `UpgradeWizard` or `SaveAs` |

### Paths

| | **Upgrade Wizard** | **Save-As** (`saveAsNativeArtifact`) |
|---|---|---|
| Mechanics | in place, per workspace, run by the owner (UI, preview) | creates a Gen2 copy, API-driven (the Microsoft accelerator) |
| Dataflow id | **kept** | **new**: every consumer must be re-pointed |
| Schedule | kept | copied **disabled** |
| Not carried over | incremental refresh configuration, cached data | CDM folders, bring-your-own storage, enhanced compute, DirectQuery, AI insights, incremental refresh |

The recommended path is the configured preference (`p_preferred_path = wizard | saveas`), otherwise the path with the least severe status, then the lowest remediation
effort; the wizard wins ties (no id change, no rebind). `p_wizard_available = False` forces Save-As.

### Alternatives to the Upgrade Wizard

The wizard is the first choice, but it is not always available or suitable: the dataflow is **Unsupported** on the wizard path, the wizard reports *Upgrade unavailable* (for example you are not the owner and cannot take over), or you
want to **keep the Gen1 dataflow** (a wizard upgrade replaces it and cannot be reverted). Microsoft describes the other approaches in
[Migrate from Dataflow Gen1 to Dataflow Gen2: Migration scenarios](https://learn.microsoft.com/fabric/data-factory/dataflow-gen2-migrate-from-dataflow-gen1-scenarios) and
[Save As](https://learn.microsoft.com/fabric/data-factory/migrate-to-dataflow-gen2-using-save-as). FDEA models the first two as paths (`UpgradeWizard`, `SaveAs`) and falls back to `ManualRebuild` when neither works; the others are
rebuild and re-architecture options that you apply from the findings.

| Option | When to use it | What to know |
|---|---|---|
| **Save As** (a Gen2 CI/CD copy; per dataflow in the portal, in bulk through the REST API) | the wizard is unavailable or unsuitable, or the Gen1 dataflow must stay in place | new dataflow id: re-point the consumers (`Assessment_RebindWorklist`); some Gen1 settings are not copied (see the table above) |
| **Power Query template** (export the queries from Gen1, import them into a new Gen2) | a dataflow that neither path accepts (`ManualRebuild`), or when you want to clean up while migrating | templates work with Gen1 and Gen2; you recreate the destination, schedule and incremental refresh yourself |
| **Re-point consumers to the new dataflow** (Microsoft scenario 1) | personal or team dataflows in one workspace, kept as a source for existing semantic models, Excel files or Dataverse tables | update the workspace and dataflow ids in the consumers' queries (`PowerPlatform.Dataflows`); parameterized semantic models can be updated through the REST API |
| **Replace linked tables with OneLake shortcuts** (Microsoft scenario 2) | composable dataflows with linked tables across workspaces (`LNK-001`, `LNK-002`, cross-workspace links) | output the tables to a Lakehouse and read them with `Lakehouse.Contents`; removes the dependency on linked-table refresh |
| **Output to a Warehouse** (Microsoft scenario 3) | shared tables that need row-level or column-level security or data masking | read with `Fabric.Warehouse`; access is granted through the SQL granular permissions |

Microsoft recommends modernizing rather than copying: Dataflow Gen2 offers fast copy, a better incremental refresh and data destinations, and CU consumption differs from Gen1, so validate it with a pilot. FDEA does not detect these
options automatically: read the findings of a dataflow (`Assessment_UnsupportedFeatures`) and pick the option that fits.

### Main rules per path

`N` = Needs attention, `U` = Unsupported, `D` = Deferred, `-` = no effect. The complete list, with detection and remediation text, is in the [rule catalog](rule-catalog.md).

| Rule | Signal | Wizard | Save-As |
|---|---|---|---|
| `CAP-001` | workspace not on a Fabric-capable capacity (Pro, PPU, A/EM, no capacity) | N | N |
| `STO-001` | bring-your-own ADLS storage (`modelUrl`, workspace storage) | N | **U** |
| `ECE-001` | enhanced compute engine on | N | N |
| `DQ-001` | a downstream semantic model uses DirectQuery | N | N |
| `INC-001` | incremental refresh policy on an entity | N | N |
| `LNK-001` / `LNK-002` | contains linked entities / read by other dataflows | N | N |
| `LNK-003` | a consumer uses the legacy `PowerBI.Dataflows` connector | N | N |
| `QRY-001` | more than 50 enabled queries | N | N |
| `QRY-002` | 40 to 50 enabled queries (information only: weight 0, no effect on class, score or effort) | - | - |
| `CON-001` | unresolved data source connections | **U** | N |
| `GW-001` | source through a gateway | N | N |
| `OWN-001` | owner missing or disabled | N | N |
| `NAM-001`, `NAM-002` | unsupported characters, duplicate name | N | - |
| `SCH-001` / `SCH-002` | more than 48 runs a day / schedule copied disabled | N / - | N / N |
| `TRG-001` | refreshes triggered through the API | N | N |
| `PIP-001`, `REB-001` | pipeline activities, downstream items to re-point | - | N |
| `AIX-001` | AI Insights or Azure ML functions | N | N |
| `STA-001`, `REF-001`, `SEQ-001` | stale, failing, blocked by an upstream | D | D |

A rule is `Verified` when it comes from Microsoft documentation (wizard assessment table, Gen1/Gen2 differences, Gen2 limits) and `Heuristic` when it infers a documented
limitation from indirect signals. `Assessment_RuleCatalog` and `assessment_confidence` (High / Medium / Low, from the completeness of the definition, compute-engine and refresh data) make that visible.

### Linked dataflows and the refresh cascade

Linked dataflows refresh differently in Gen1 depending on where they live, and not at all in Gen2:

| Situation | Gen1 automatic cascade |
|---|---|
| Parent and child in the **same workspace** | **Yes**: refreshing the parent (source) refreshes the child (destination), in one transaction (if the child fails, the parent refresh fails as well) |
| Parent and child in **different workspaces** | **No**: the link behaves like an external data source, the child takes the latest data of the parent when it refreshes |
| Child refreshed manually | Does not refresh the parent |

Dataflow Gen2 does not cascade at all. Before a dataflow with linked entities is migrated, decide how its refreshes are chained afterwards: a pipeline that refreshes the dataflows in order, or schedules staggered so that each one
starts after its source. A same-workspace chain that "just worked" in Gen1 is the case that silently changes. `LNK-001` and `LNK-002` flag the dependency, `Assessment_DataflowDependencies` and the `Crosses Workspace` column tell you whether it crosses workspaces
(and so was not cascading in Gen1 either), and `Assessment_MigrationBacklog` orders the upstream dataflows first. Sources: Microsoft Learn,
[Link tables between dataflows](https://learn.microsoft.com/power-query/dataflows/linked-tables#refresh-logic-of-linked-tables) and the [Upgrade Wizard limitations](https://learn.microsoft.com/fabric/data-factory/migrate-to-dataflow-gen2-using-upgrade-wizard).

## 3. Complexity scoring algorithm

`complexity_score` (0-100) is the weighted average of six component scores, each between 0 and 100. Weights are `p_score_weights` (JSON); the defaults sum to 100.

| Component | Weight | Points (capped at 100) |
|---|---|---|
| `dependencies` | 20 | 12 per upstream dataflow, 12 per downstream dataflow, 6 per downstream semantic model, 2 per report, 4 per dashboard, 4 per other asset (transitive), +15 if a direct link crosses workspaces, +25 if the dataflow is in a dependency cycle |
| `refresh_criticality` | 15 | schedule frequency (1/day: 15, 4: 30, 12: 40, 24: 50) + p95 duration (5 min: 5, 30 min: 15, 1 h: 25) + external triggers 15 + business criticality (High 10, Medium 5) |
| `gateway` | 15 | uses a gateway 50, on-premises source +25, more than one gateway +10 each (max 25) |
| `source_complexity` | 15 | number of sources (1: 10, 3: 25, 6: 40, 11: 55) + number of source types (2: 10, 4: 20, 7: 30) + heaviest category (Fabric 0, database / storage 5, file / SaaS 10, web / other 15) |
| `unsupported` | 25 | sum of the weights of the rule hits that need action on the recommended path (each extra entity of a rule +25%, up to x2); informational findings such as `QRY-002` weigh 0 |
| `technical` | 10 | enabled queries (10: 10, 25: 25, 40: 40, 50: 50) + custom functions (1: 5, 4: 10, 10: 15) + parameters (1: 3, 5: 7, 10: 10) + native queries (1: 5, 5: 10) + script size (20 k characters: 5, 100 k: 10) + computed and linked entities (max 5) |

Bands: **Low** < 20, **Medium** < 40, **High** < 60, **Very High** >= 60 (`p_score_weights = {"bands": {"high": 45}}` moves a threshold; the checklist's *Faible / Moyenne / Elevee*
are Low / Medium / High).

**Worked example.** A dataflow with one upstream dataflow, two semantic models and three reports downstream, two runs a day on weekdays, a p95 of 25 minutes, High criticality, a
gateway to an on-premises database, six sources of three types (one file, one web), 28 queries, two custom functions, one parameter, and the findings incremental refresh on two
entities, a gateway, linked entities, an enabled schedule and three consumers to re-point:

| Component | Wizard path | Save-As path |
|---|---|---|
| dependencies | 12 + 12 + 6 = 30 | 30 |
| refresh criticality | 15 + 5 + 10 = 30 | 30 |
| gateway | 50 + 25 = 75 | 75 |
| source complexity | 40 + 10 + 15 = 65 | 65 |
| unsupported | 30 x 1.25 + 15 + 25 = 77.5 | 100 (127.5 capped: adds the schedule 5 and `REB-001` 30 x 1.5) |
| technical | 25 + 5 + 3 + 5 + 1 = 39 | 39 |
| **score** | **54.8 - High** | **60.4 - Very High** |
| effort | 4.0 + 1.5 + 0.5 + 1.0 = **7.0 days** | 8.0 + 3.85 = **11.85 days** |

## 4. Effort and cost estimate

* **Effort (person-days)** = base of the band (Low 0.5, Medium 1.5, High 4, Very High 8) + the remediation effort of every rule that needs action on the recommended path.
  A rule costs `effort_days x (1 + 0.5 per additional entity, up to 10)`; `REB-001` costs 0.25 day per downstream item to re-point (up to 40). Retirement candidates cost 0.25 day.
* **Indicative Gen2 compute** (`est_gen2_cu_seconds_per_run`, `est_gen2_cu_hours_per_month`): standard dataflow compute is 12 CU-seconds per second for the first 10 minutes of a refresh and
  1.5 afterwards, applied to the Gen1 average duration and the schedule. In the example: 1 200 s -> 8 100 CU-s per run, 97.5 CU-hours per month. Gen2 durations differ: validate
  with the pilot and use the capacity metrics app.

## 5. Migration waves

| Wave | Rule |
|---|---|
| **0 - Pilot** | up to `p_pilot_size` (10) dataflows chosen among the quick wins: isolated (no linked dataflow), **never Prod** (Dev is preferred over Test and Unknown), healthy refresh, one to five consumers preferred, then lowest complexity; at most `max(1, p_pilot_size / 3)` per workspace so that several owners and sources are exercised. Dataflows with a **verified** refresh baseline (a successful run observed) come first; dataflows without refresh history only fill the slots left, and `wave_rationale` says "refresh baseline not verified" |
| **1 - Quick wins** | Ready, Low complexity, business criticality not High |
| **2 - Standard** | Ready with Medium complexity or High criticality; Needs Attention with Low / Medium complexity and criticality not High |
| **3 - Complex or critical** | Ready with High / Very High complexity; Needs Attention with High / Very High complexity or High criticality |
| **4 - Remediate or rebuild** | Unsupported |
| **99 - Backlog** | Deferred |

Then three adjustments:

1. **Dependency order.** Among the planned waves (0-4) a dataflow is never planned before the dataflows it reads: `wave(d) >= wave(u)` for every upstream `u` that is not blocking. Dependency
   cycles are condensed into one unit and migrate together (Tarjan strongly connected components, then topological order); `dependency_level` is the longest upstream chain.
   A dataflow behind an Unsupported or Deferred upstream is itself blocked (`SEQ-001`, wave 99): the backlog is **outside** the order, so the members of a blocked cycle can sit in
   different waves (4 and 99) under the same `migration_group_id`.
2. **Groups and batches.** Linked dataflows form a `migration_group_id` (connected component). `batch_id` = `W<wave>-<workspace id>` (the full workspace id, so that two workspaces never
   share a batch), because the Upgrade Wizard works on one workspace at a time. A dataflow without a known workspace is grouped under `unknown`. `environment` of a batch is Dev, Test
   or Prod when all its dataflows agree, **Mixed** when they do not, Unknown otherwise.
3. **Pins.** `pin_wave` (0-4) in the governance CSV wins for Ready and Needs Attention dataflows; any other value is ignored (use `defer` to send a dataflow to the backlog). Conflicts
   with an upstream wave are stated in `wave_rationale`.

`Assessment_WavePlan` aggregates the batches (counts per class, effort, highest complexity, downstream items); `Assessment_MigrationBacklog` is the ordered work queue:
`seq`, `can_start_now` (Ready and every upstream already migrated), `blocked_by`, `pre_migration_actions`, `post_migration_actions`.

## 6. Recommendations

Each dataflow gets `recommendation` (plain language), `top_actions` (most severe first, at most six) and, in the backlog, the actions split **before** and **after** the migration. Examples:

* *Ready: upgrade in place with the Upgrade Wizard (keeps the dataflow id and name; the owner runs it for the whole workspace batch). Planned in Wave 1 - Quick wins.*
* *Needs attention before migrating - 2 finding(s) to resolve on Save-As: Unresolved data source connections; Workspace not on a Fabric-capable capacity. Then create the Gen2 copy with Save-As (new id: re-point the downstream items and re-enable the schedule). Planned in Wave 2 - Standard.*
* *Candidate for retirement - Stale and unused: last successful refresh 2026-03-02, no downstream consumers. Confirm with the owner and decommission instead of migrating.*

## 7. Calibration

The wizard is a UI feature with no API, so the notebook **predicts** its verdict. Record what the wizard really says for a sample of workspaces in the governance CSV
(`wizard_status_actual`) and compare it with `predicted_wizard_status`; disagreements point to a rule to tune (`enabled`, impact or weight in the rule catalog cell) or to a missing signal.
Heuristic rules are the first suspects (`CMP-001`, `TRG-001`, the `computeEngineBehavior` interpretation).

## 8. Not assessed

* Data volumes, row counts and the content of the data (no API).
* Consumers that are not Fabric / Power BI items known to the Scanner: notebooks, external applications, Excel files, Power Automate flows (only API-triggered refreshes are hinted by `TRG-001`).
* Transformations that behave differently in Gen2 beyond the documented limits: the pilot and the post-migration validation remain mandatory.
* Gen2 dataflows (out of scope), Dataflow Gen2 CI/CD specifics, and GCC tenants (the wizard is not available there).
