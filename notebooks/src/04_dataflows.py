# %% [markdown]
# # 4. Dataflow discovery
# **Sources, in order of authority**
# 1. Scanner (`dataflows[]`): identity, owner, modified, endorsement, sensitivity label, schedule, tags, data source usages,
#    `upstreamDataflows`, plus the semantic models / reports / dashboards / datamarts that give the **downstream** lineage.
# 2. Power BI `admin/dataflows` (tenant wide, 200 req/h): `modelUrl` (attached storage = BYOSA indicator) and a complete Gen1 id list.
# 3. Fabric `admin/items?type=Dataflow` (tenant wide, 10 000 per page): Gen2 items, creator, last update, tags.
#
# **Generation resolution.** An explicit `generation` wins; otherwise an id known to the Power BI APIs is Gen1; an id seen
# only by the Fabric items API is Gen2. A Gen1 -> Gen2 flip of the same id (in-place Upgrade Wizard) is recorded in
# `type_changed_at` and feeds the migration burn-down. Items of deleted workspaces are ignored (the items API still reports them as active).
# **Deletions** are only recorded when the tenant-wide lists were complete.

# %%
def phase_admin_dataflows(ctx):
    """Tenant-wide Gen1 list: modelUrl (attached storage) and an authoritative id set independent of the Scanner scope."""
    pbi = ctx.clients["pbi"]
    disc = ctx.state.setdefault("discovery", {"scanner": {}, "admin_pbi": {}, "admin_items": {}, "complete": {}})
    rows, found = [], {}
    for d in pbi.paginate("admin/dataflows", style="skip", page_size=1000, buckets=("pbi_admin_dataflows",)):
        did, wid = gid(d.get("objectId")), gid(d.get("workspaceId"))
        if not did:
            continue
        generation = coerce_value("INT", d.get("generation"))
        found[did] = {"workspace_id": wid, "generation": generation, "name": d.get("name"), "description": d.get("description"),
                      "configured_by": d.get("configuredBy"), "model_url": d.get("modelUrl"), "storage_type": storage_type_of(d.get("modelUrl"))}
        row = {"dataflow_id": did, "workspace_id": wid, "model_url": d.get("modelUrl"), "storage_type": storage_type_of(d.get("modelUrl"))}
        if generation:
            row.update(generation=generation, dataflow_type="Gen2" if generation == 2 else "Gen1")
        rows.append(row)
    disc["admin_pbi"] = found
    disc["complete"]["admin_pbi"] = True
    known = {r["dataflow_id"] for r in ctx.lake.read("Assessment_Dataflows")}
    merge_by_signature(ctx.lake, "Assessment_Dataflows", [r for r in rows if r["dataflow_id"] in known], update_cols=["model_url", "storage_type"])
    return {"rows_in": len(rows), "rows_out": len(found), "message": f"{sum(1 for r in rows if r['storage_type'] == 'CustomADLS')} with attached ADLS storage"}


def phase_admin_items(ctx):
    """Tenant-wide Fabric items of type Dataflow (Gen2). Ghost items of deleted workspaces are dropped."""
    fabric = ctx.clients["fabric"]
    disc = ctx.state.setdefault("discovery", {"scanner": {}, "admin_pbi": {}, "admin_items": {}, "complete": {}})
    buckets = ("fabric_admin_items", "fabric_platform")
    try:
        items = list(fabric.paginate("admin/items", params={"type": "Dataflow"}, items_key="itemEntities", buckets=buckets))
    except ApiError as exc:
        if exc.status != 400:
            raise
        ctx.logger.warning("type filter rejected (%s): listing all items and filtering locally", exc.code)
        items = [i for i in fabric.paginate("admin/items", items_key="itemEntities", buckets=buckets) if i.get("type") == "Dataflow"]
    active = {wid for wid, r in ctx.state["workspaces"].items() if r.get("state") == "Active"} if ctx.state.get("workspaces") else None
    kept, ghosts = {}, 0
    for item in items:
        wid, did = gid(item.get("workspaceId")), gid(item.get("id"))
        if not did:
            continue
        if active is not None and wid not in active:
            ghosts += 1
            continue
        kept[did] = item
    disc["admin_items"] = kept
    disc["complete"]["admin_items"] = True
    return {"rows_in": len(items), "rows_out": len(kept), "message": f"{ghosts} items of deleted or unknown workspaces ignored"}


def resolve_generation(did, scanner, admin_pbi, items):
    explicit = (scanner.get(did) or {}).get("generation") or (admin_pbi.get(did) or {}).get("generation")
    if explicit == 2:
        return "Gen2", 2
    if explicit == 1 or did in scanner or did in admin_pbi:
        return "Gen1", 1
    if did in items:
        return "Gen2", 2
    return "Unknown", None


def cascade_workspace_deletions(ctx):
    """Dataflows, sources and lineage edges of workspaces that left the tenant listing are flagged as deleted with them.

    Derived from the Lakehouse (workspace rows already flagged is_deleted), so an interrupted run is repaired by the next one.
    """
    lake = ctx.lake
    dead = {r["workspace_id"] for r in lake.read("Assessment_Workspaces", include_deleted=True) if r.get("is_deleted")}
    if not dead:
        return {"dataflows": 0, "sources": 0, "edges": 0}
    flows = [{"dataflow_id": r["dataflow_id"]} for r in lake.read("Assessment_Dataflows", where={"workspace_id": dead})]
    sources = [{"dataflow_id": r["dataflow_id"], "source_key": r["source_key"]} for r in lake.read("Assessment_DataflowSources", where={"workspace_id": dead})]
    edge_ids = {r["edge_id"] for column in ("src_workspace_id", "dst_workspace_id") for r in lake.read("Assessment_Lineage", where={column: dead})}
    for table, keys in (("Assessment_Dataflows", flows), ("Assessment_DataflowSources", sources), ("Assessment_Lineage", [{"edge_id": e} for e in sorted(edge_ids)])):
        if keys:
            lake.mark_deleted(table, keys)
    if flows or sources or edge_ids:
        ctx.logger.info("%d deleted workspaces: %d dataflows, %d sources and %d lineage edges flagged as deleted with them", len(dead), len(flows), len(sources), len(edge_ids))
    return {"dataflows": len(flows), "sources": len(sources), "edges": len(edge_ids)}


def phase_reconcile_dataflows(ctx):
    cfg, lake, disc = ctx.cfg, ctx.lake, ctx.state.get("discovery") or {}
    scanner, admin_pbi, items = disc.get("scanner", {}), disc.get("admin_pbi", {}), disc.get("admin_items", {})
    existing = {r["dataflow_id"]: r for r in lake.read("Assessment_Dataflows")}
    in_scope = ctx.state.get("scope_ws_ids")
    rows = []
    for did in set(scanner) | set(admin_pbi) | set(items):
        wid = (scanner.get(did) or admin_pbi.get(did) or {}).get("workspace_id") or gid((items.get(did) or {}).get("workspaceId"))
        if in_scope is not None and wid not in in_scope:
            continue
        dtype, generation = resolve_generation(did, scanner, admin_pbi, items)
        prev = existing.get(did) or {}
        sources = [s for s, present in (("Scanner", did in scanner), ("AdminDataflows", did in admin_pbi), ("AdminItems", did in items)) if present]
        row = {"dataflow_id": did, "workspace_id": wid, "dataflow_type": dtype, "generation": generation, "dataflow_key": stable_hash64(did),
               "first_seen_dataflow_type": prev.get("first_seen_dataflow_type") or dtype, "discovery_sources": sources,
               "type_changed_at": ctx.clock.now() if prev.get("dataflow_type") not in (None, dtype) and dtype != "Unknown" else prev.get("type_changed_at")}
        if did in admin_pbi and did not in scanner:
            adm = admin_pbi[did]
            row.update(dataflow_name=adm.get("name"), description=adm.get("description"), owner_upn=adm.get("configured_by"),
                       owner_source="configuredBy" if adm.get("configured_by") else None, model_url=adm.get("model_url"),
                       storage_type=adm.get("storage_type"))
        elif did in items and did not in scanner:
            item = items[did]
            creator = dig(item, "creatorPrincipal", "userDetails", "userPrincipalName")
            row.update(dataflow_name=item.get("name"), description=item.get("description"), modified_at=parse_ts(item.get("lastUpdatedDate")),
                       owner_upn=creator, owner_source="creatorPrincipal" if creator else None,
                       tags=[t.get("displayName") for t in (item.get("tags") or []) if isinstance(t, dict)])
        rows.append(row)
    merge_by_signature(lake, "Assessment_Dataflows", rows)

    complete = disc.get("complete", {})
    can_delete = (complete.get("admin_items") or not cfg.collect_gen2) and (complete.get("admin_pbi") or not cfg.collect_gen1)
    present = set(scanner) | set(admin_pbi) | set(items)
    gone = [{"dataflow_id": did} for did, r in existing.items() if did not in present and (in_scope is None or r.get("workspace_id") in in_scope)]
    if gone and can_delete:
        lake.mark_deleted("Assessment_Dataflows", gone)
    elif gone:
        ctx.logger.warning("%d dataflows missing from the scan but the tenant-wide lists were incomplete: not flagged as deleted", len(gone))
    cascaded = cascade_workspace_deletions(ctx)
    current = {r["dataflow_id"]: r for r in lake.read("Assessment_Dataflows")}
    counts = defaultdict(Counter)
    for r in current.values():
        counts[r["workspace_id"]][r["dataflow_type"]] += 1
    if counts:
        lake.merge("Assessment_Workspaces", [{"workspace_id": w, "gen1_dataflow_count": c["Gen1"], "gen2_dataflow_count": c["Gen2"]} for w, c in counts.items()],
                   update_cols=["gen1_dataflow_count", "gen2_dataflow_count"])
    caps = ctx.state.get("capacities", {})
    by_cap = defaultdict(Counter)
    for w, c in counts.items():
        cap = (ctx.state.get("workspaces", {}).get(w) or {}).get("capacity_id")
        if cap:
            by_cap[cap].update(c)
    if by_cap:
        lake.merge("Assessment_Capacities", [{"capacity_id": cid, "gen1_dataflow_count": c["Gen1"], "gen2_dataflow_count": c["Gen2"]} for cid, c in by_cap.items() if cid in caps],
                   update_cols=["gen1_dataflow_count", "gen2_dataflow_count"])
    flips = sum(1 for r in current.values() if r.get("type_changed_at") and r.get("first_seen_dataflow_type") == "Gen1" and r["dataflow_type"] == "Gen2")
    types = Counter(r["dataflow_type"] for r in current.values())
    return {"rows_in": len(present), "rows_out": len(current),
            "message": f"Gen1={types['Gen1']} Gen2={types['Gen2']} Unknown={types['Unknown']} migrated-in-place={flips} removed-with-their-workspace={cascaded['dataflows']}"}

# %% [markdown]
# ### Run - section 4: dataflow discovery

# %% tags=["run"]
run_steps(ctx, sections={4})
