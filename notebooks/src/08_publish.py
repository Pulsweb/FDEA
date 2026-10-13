# %% [markdown]
# # 8. Delta persistence
# Silver tables are written by the collection phases (partial-column `MERGE` + soft deletes). This section publishes the **Gold**
# tables produced by the analysis:
#
# * full snapshot per run (`INSERT OVERWRITE`): Gold is derived data, always consistent with the latest Silver and rules,
# * row counts are verified after each write (a mismatch fails the phase),
# * `OPTIMIZE ... ZORDER BY` (+ optional `VACUUM`) keeps files compact; V-Order and optimized writes are enabled by the storage layer.
#
# | Layer | Tables | Write pattern |
# |---|---|---|
# | Silver (current state) | Capacities, Workspaces, Dataflows, DataflowSources, Lineage | `MERGE` by business key, soft delete |
# | Silver (history) | RefreshHistory (partitioned by month) | `MERGE` by (dataflow, refresh id) |
# | Gold (assessment) | MigrationReadiness, UnsupportedFeatures, DataflowDependencies, LineageNodes, DataflowSummary, WavePlan, MigrationBacklog, RebindWorklist, RuleCatalog | snapshot per run |
# | Control | Checkpoint, RunLog, ErrorLog, RunSummary, GovernanceOverrides | merge / append |

# %%
def expected_row_count(table_name, rows):
    keys = TABLES[table_name].keys
    return len({tuple(r.get(k) for k in keys) for r in rows})


def phase_publish(ctx):
    analysis = ctx.state.get("analysis")
    if not analysis:
        raise DeaError("no analysis result to publish: the analysis phase did not run or failed")
    written, problems = {}, []
    for name in GOLD_TABLES:
        rows = analysis["tables"][name]
        ctx.lake.replace(name, rows)
        written[name] = ctx.lake.count(name)
        expected = expected_row_count(name, rows)
        ctx.logger.info("%-34s %7d rows", name, written[name])
        if written[name] != expected:
            problems.append(f"{name}: {written[name]} rows stored, {expected} expected")
            ctx.record_error("publish", "Table", name, None, message=problems[-1])
    ctx.state["published"] = written
    metrics = {"rows_in": sum(expected_row_count(n, analysis["tables"][n]) for n in GOLD_TABLES), "rows_out": sum(written.values()),
               "message": f"{len(written)} Gold tables published"}
    if problems:
        metrics.update(status="Failed", message="; ".join(problems))
    return metrics


def phase_maintenance(ctx):
    """OPTIMIZE (Z-ORDER where defined) and optional VACUUM. Maintenance never fails the run; the message states what really ran
    (Partial when a table could not be optimized)."""
    hours = ctx.cfg.vacuum_retention_hours
    optimized = vacuumed = failed = 0
    for name in TABLES:
        done = ctx.lake.maintain(name, hours)
        if done is None:                       # this lake has nothing to maintain (in-memory)
            continue
        optimized += "OPTIMIZE" in done
        vacuumed += "VACUUM" in done
        failed += "OPTIMIZE" not in done
    return {"rows_in": len(TABLES), "rows_out": optimized, "status": "Partial" if failed else "Succeeded",
            "message": f"{optimized} of {len(TABLES)} tables optimized, {vacuumed} vacuumed" + (f" (retention {hours} h)" if hours else "")}

# %% [markdown]
# ### Run - section 8: Delta persistence

# %% tags=["run"]
run_steps(ctx, sections={8})
