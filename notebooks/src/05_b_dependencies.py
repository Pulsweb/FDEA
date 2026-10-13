# %% [markdown]
# ## 5.2 Complementary collectors (all budgeted, checkpointed and individually switchable)
# | Phase | API | Default | Purpose |
# |---|---|---|---|
# | `compute_engine` | `groups/{ws}/dataflows` (workspace member) | on | Enhanced compute engine setting (rule ECE-001) |
# | `admin_datasources` | `admin/dataflows/{id}/datasources` | off | Sources for dataflows the Scanner did not describe |
# | `upstream_verify` | `admin/groups/{ws}/dataflows/{id}/upstreamDataflows` (200 req/h) | off | Cross-check of the Scanner lineage |
# | `pipelines` | `admin/items?type=DataPipeline` + `getDefinition` | off | Pipelines whose `RefreshDataFlow` activity runs a dataflow (control edges) |
# | `item_connections` | `workspaces/{ws}/items/{id}/connections` (read+write) | off | Gen2 connection details, credential type, gateway type |
# | `owners_graph` | Microsoft Graph `users/{upn}` (`User.Read.All`) | off | Owner display name and disabled / deleted accounts (rule OWN-001) |
# | `source_rollup` | gateways (security-trimmed) | on | Per-dataflow source and gateway counts |
#
# A **circuit breaker** stops a phase after 25 consecutive denied calls with no success, so a missing workspace permission
# costs 25 calls instead of the whole quota. Denied entities are not retried for `p_denied_retry_days`.

# %%
def phase_compute_engine(ctx):
    cfg, pbi, lake = ctx.cfg, ctx.clients["pbi"], ctx.lake
    scope = ctx.state.get("scope_ws_ids")
    picked = []
    for ws in lake.read("Assessment_Workspaces", where={"workspace_id": scope} if scope else None):
        if not ws.get("gen1_dataflow_count"):
            continue
        collect, reason = ctx.checkpoints.decide("Workspace", ws["workspace_id"], "compute_engine", ttl_hours=168)
        if collect:
            picked.append((_RANK.get(reason, 5), ws["workspace_id"], ws))
    picked.sort(key=lambda p: p[:2])
    candidates = [p[2] for p in picked[:cfg.budget_definitions_per_run]]

    def work(row):
        body = pbi.get_json(f"groups/{row['workspace_id']}/dataflows", buckets=("pbi_dataflow",))
        return {gid(d.get("objectId")): d.get("computeEngineBehavior") or "NotConfigured" for d in body.get("value", [])}

    def persist(outcomes):
        rows = [{"dataflow_id": did, "compute_engine_behavior": value} for _, result, status in outcomes if result for did, value in result.items()]
        known = {r["dataflow_id"] for r in lake.read("Assessment_Dataflows")}
        rows = [r for r in rows if r["dataflow_id"] in known]
        if rows:
            lake.merge("Assessment_Dataflows", rows, update_cols=["compute_engine_behavior"])

    done = run_budgeted(ctx, "compute_engine", candidates, work,
                        lambda row, result: ctx.checkpoints.mark("Workspace", row["workspace_id"], "compute_engine", "Done"), persist,
                        entity_type="Workspace", key_of=lambda r: r["workspace_id"])
    return {"rows_in": len(picked), "rows_out": done, "message": f"{done} workspaces read, {max(0, len(picked) - len(candidates))} left"}

# %%
def phase_admin_datasources(ctx):
    cfg, pbi, lake = ctx.cfg, ctx.clients["pbi"], ctx.lake
    covered = {s["dataflow_id"] for s in lake.read("Assessment_DataflowSources")}
    candidates, eligible = select_candidates(ctx, "datasources", lambda r: r["dataflow_type"] == "Gen1" and r["dataflow_id"] not in covered,
                                             cfg.budget_datasources_per_run, ttl_hours=168)

    def work(row):
        body = pbi.get_json(f"admin/dataflows/{row['dataflow_id']}/datasources", buckets=("pbi_admin_dataflow_datasources",))
        return [build_source_row(row["dataflow_id"], row["workspace_id"], gid(d.get("datasourceId")),
                                 {"type": d.get("datasourceType"), "details": d.get("connectionDetails") or {}, "gateway_id": gid(d.get("gatewayId"))}, "AdminDatasources")
                for d in body.get("value", [])]

    def persist(outcomes):
        rows = [s for _, result, _s in outcomes if result for s in result]
        if rows:
            merge_by_signature(lake, "Assessment_DataflowSources", rows)

    done = run_budgeted(ctx, "datasources", candidates, work, lambda row, result: ctx.checkpoints.mark("Dataflow", row["dataflow_id"], "datasources", "Done"), persist)
    return {"rows_in": eligible, "rows_out": done, "message": f"{done} dataflows read, {max(0, eligible - len(candidates))} left"}


def phase_upstream_verify(ctx):
    cfg, pbi, lake = ctx.cfg, ctx.clients["pbi"], ctx.lake
    candidates, eligible = select_candidates(ctx, "upstream", lambda r: r["dataflow_type"] == "Gen1", cfg.budget_datasources_per_run, ttl_hours=168)

    def work(row):
        body = pbi.get_json(f"admin/groups/{row['workspace_id']}/dataflows/{row['dataflow_id']}/upstreamDataflows", buckets=("pbi_admin_dataflow_upstream",))
        return [edge_row("Dataflow", u.get("targetDataflowId"), u.get("groupId"), None, "Dataflow", row["dataflow_id"], row["workspace_id"], row.get("dataflow_name"),
                         "LinkedEntity", evidence="AdminUpstream") for u in body.get("value", [])]

    def persist(outcomes):
        done_ids = {r["dataflow_id"] for r, result, _ in outcomes if result is not None}
        edges = [e for _, result, _s in outcomes if result for e in result]
        fresh = {e["edge_id"] for e in edges}
        stale = [{"edge_id": e["edge_id"]} for e in lake.read("Assessment_Lineage", where={"dst_id": done_ids})
                 if e.get("evidence") == "AdminUpstream" and e["edge_id"] not in fresh] if done_ids else []
        if stale:
            lake.mark_deleted("Assessment_Lineage", stale)
        if edges:
            lake.merge("Assessment_Lineage", edges)

    done = run_budgeted(ctx, "upstream", candidates, work, lambda row, result: ctx.checkpoints.mark("Dataflow", row["dataflow_id"], "upstream", "Done"), persist)
    return {"rows_in": eligible, "rows_out": done, "message": f"{done} dataflows verified, {max(0, eligible - len(candidates))} left"}

# %%
def find_dataflow_activities(node, found=None):
    """All RefreshDataFlow activities anywhere in a pipeline definition (containers nest activities arbitrarily)."""
    found = [] if found is None else found
    if isinstance(node, dict):
        props = node.get("typeProperties")
        if str(node.get("type", "")).lower() == "refreshdataflow" and isinstance(props, dict) and props.get("dataflowId"):
            found.append({"dataflow_id": gid(props["dataflowId"]), "workspace_id": gid(props.get("workspaceId")), "activity": node.get("name")})
        for value in node.values():
            find_dataflow_activities(value, found)
    elif isinstance(node, list):
        for value in node:
            find_dataflow_activities(value, found)
    return found


def phase_pipelines(ctx):
    cfg, fabric, lake = ctx.cfg, ctx.clients["fabric"], ctx.lake
    buckets = ("fabric_admin_items", "fabric_platform")
    try:
        items = list(fabric.paginate("admin/items", params={"type": "DataPipeline"}, items_key="itemEntities", buckets=buckets))
    except ApiError as exc:
        if exc.status != 400:
            raise
        items = [i for i in fabric.paginate("admin/items", items_key="itemEntities", buckets=buckets) if i.get("type") == "DataPipeline"]
    scope = ctx.state.get("scope_ws_ids")
    rows = [{"id": gid(i["id"]), "workspace_id": gid(i.get("workspaceId")), "name": i.get("name"), "modified": str(i.get("lastUpdatedDate") or "none")}
            for i in items if i.get("id") and (not scope or gid(i.get("workspaceId")) in scope)]
    picked = []
    for row in rows:
        collect, reason = ctx.checkpoints.decide("Pipeline", row["id"], "pipelines", version=row["modified"])
        if collect:
            picked.append((_RANK.get(reason, 5), row["id"], row))
    picked.sort(key=lambda p: p[:2])
    candidates = [p[2] for p in picked[:cfg.budget_datasources_per_run]]

    def work(row):
        resp = fabric.request("POST", f"workspaces/{row['workspace_id']}/dataPipelines/{row['id']}/getDefinition", json_body={}, ok=(200, 202), buckets=("fabric_platform",))
        body = fabric.lro(resp)
        parts = decode_parts(body.get("definition") or body)
        if "pipeline-content.json" not in parts:
            raise EmptyDefinition("the pipeline definition has no pipeline-content.json part", endpoint="dataPipelines/getDefinition", retryable=True)
        content = json.loads(parts["pipeline-content.json"] or "{}")
        return find_dataflow_activities(content)

    def persist(outcomes):
        done_ids = {r["id"] for r, result, _ in outcomes if result is not None}
        edges = []
        for row, result, _ in outcomes:
            for ref in result or []:
                edges.append(edge_row("Dataflow", ref["dataflow_id"], ref["workspace_id"], None, "DataPipeline", row["id"], row["workspace_id"], row["name"],
                                      "PipelineActivity", kind="CONTROL", evidence="PipelineDefinition", attributes={"activity": ref["activity"]}))
        fresh = {e["edge_id"] for e in edges}
        stale = [{"edge_id": e["edge_id"]} for e in lake.read("Assessment_Lineage", where={"dst_id": done_ids})
                 if e.get("evidence") == "PipelineDefinition" and e["edge_id"] not in fresh] if done_ids else []
        if stale:
            lake.mark_deleted("Assessment_Lineage", stale)
        if edges:
            lake.merge("Assessment_Lineage", edges)

    done = run_budgeted(ctx, "pipelines", candidates, work,
                        lambda row, result: ctx.checkpoints.mark("Pipeline", row["id"], "pipelines", "Done", version=row["modified"]), persist,
                        entity_type="Pipeline", key_of=lambda r: r["id"])
    return {"rows_in": len(rows), "rows_out": done, "message": f"{done} pipelines inspected of {len(rows)}"}

# %%
_CONNECTIVITY_GATEWAY_TYPE = {"OnPremisesGateway": "OnPremises", "OnPremisesGatewayPersonal": "OnPremises", "VirtualNetworkGateway": "VirtualNetwork",
                              "StreamingVirtualNetworkGateway": "VirtualNetwork"}


def connection_to_source(row, conn):
    details = conn.get("connectionDetails") or {}
    ctype, path = details.get("type") or "Unknown", str(details.get("path") or "")
    parsed = {"path": path}
    if ctype.lower() in ("sql", "azuresqldatabase", "azuresql", "synapse", "postgresql", "mysql", "oracle", "snowflake"):
        server, _, database = path.partition(";")
        parsed = {"server": server, "database": database or None}
    elif path.lower().startswith("http"):
        parsed = {"url": path}
    gateway_type = _CONNECTIVITY_GATEWAY_TYPE.get(conn.get("connectivityType"))
    gateway_id = gid(conn.get("gatewayId"))
    inst = {"type": ctype, "details": {k: v for k, v in parsed.items() if v}, "gateway_id": gateway_id}
    src = build_source_row(row["dataflow_id"], row["workspace_id"], gid(conn.get("id")), inst, "ItemConnections")
    src.update(connection_id=gid(conn.get("id")), credential_type=dig(conn, "credentialDetails", "credentialType"), gateway_type=gateway_type or src["gateway_type"],
               uses_gateway=bool(gateway_id or gateway_type), is_on_premises=gateway_type == "OnPremises" or src["is_on_premises"])
    return src


def phase_item_connections(ctx):
    cfg, fabric, lake = ctx.cfg, ctx.clients["fabric"], ctx.lake
    candidates, eligible = select_candidates(ctx, "connections", lambda r: r["dataflow_type"] == "Gen2", cfg.budget_definitions_per_run, ttl_hours=168)

    def work(row):
        conns = list(fabric.paginate(f"workspaces/{row['workspace_id']}/items/{row['dataflow_id']}/connections", items_key="value", buckets=("fabric_platform",)))
        return [connection_to_source(row, c) for c in conns]

    def persist(outcomes):
        rows = [s for _, result, _s in outcomes if result for s in result]
        if rows:
            merge_by_signature(lake, "Assessment_DataflowSources", rows)

    done = run_budgeted(ctx, "connections", candidates, work, lambda row, result: ctx.checkpoints.mark("Dataflow", row["dataflow_id"], "connections", "Done"), persist)
    return {"rows_in": eligible, "rows_out": done, "message": f"{done} Gen2 dataflows read, {max(0, eligible - len(candidates))} left"}

# %%
def phase_owners_graph(ctx):
    """Owner display name and account state through Microsoft Graph (needs an SPN with User.Read.All)."""
    graph, lake = ctx.clients["graph"], ctx.lake
    rows = lake.read("Assessment_Dataflows")
    owners = sorted({(r.get("owner_upn") or "").lower() for r in rows if r.get("owner_upn") and "@" in r["owner_upn"]})
    picked = [o for o in owners if ctx.checkpoints.decide("Owner", o, "graph", ttl_hours=168)[0]][:ctx.cfg.budget_datasources_per_run]
    resolved = {}
    for upn in picked:
        try:
            body = graph.get_json(f"users/{urllib.parse.quote(upn)}", params={"$select": "displayName,accountEnabled,department"}, buckets=("graph",))
            resolved[upn] = {"owner_display_name": body.get("displayName"), "owner_enabled": body.get("accountEnabled")}
            ctx.checkpoints.mark("Owner", upn, "graph", "Done")
        except NotFoundError:
            resolved[upn] = {"owner_display_name": None, "owner_enabled": False}
            ctx.checkpoints.mark("Owner", upn, "graph", "Done")
        except (ForbiddenError, AuthError) as exc:
            ctx.logger.warning("Microsoft Graph denied (%s): grant User.Read.All to the service principal or leave p_enrich_owners_graph off", exc.status)
            ctx.record_error("owners_graph", "Owner", upn, exc.endpoint, exc=exc)
            break
    updates = [{"dataflow_id": r["dataflow_id"], **resolved[r["owner_upn"].lower()]} for r in rows if r.get("owner_upn") and r["owner_upn"].lower() in resolved]
    if updates:
        lake.merge("Assessment_Dataflows", updates, update_cols=["owner_display_name", "owner_enabled"])
    ctx.checkpoints.flush()
    return {"rows_in": len(owners), "rows_out": len(resolved), "message": f"{len(resolved)} owners resolved, {sum(1 for v in resolved.values() if v['owner_enabled'] is False)} disabled or missing"}

# %%
def phase_source_rollup(ctx):
    """Per-dataflow connectivity indicators from the source inventory; gateway names where the identity can list gateways."""
    lake, fabric = ctx.lake, ctx.clients["fabric"]
    gateways = {}
    try:
        gateways = {gid(g.get("id")): g for g in fabric.paginate("gateways", items_key="value", buckets=("fabric_platform",))}
    except ApiError as exc:
        ctx.logger.info("gateway names unavailable (%s): the gateway list is security-trimmed, ids are kept", exc.status)
    sources = lake.read("Assessment_DataflowSources")
    named = []
    for s in sources:
        g = gateways.get(s.get("gateway_id"))
        if g and not s.get("gateway_name"):
            named.append({"dataflow_id": s["dataflow_id"], "source_key": s["source_key"], "gateway_name": g.get("displayName") or g.get("name"),
                          "gateway_type": g.get("type") or s.get("gateway_type")})
    if named:
        lake.merge("Assessment_DataflowSources", named, update_cols=["gateway_name", "gateway_type"])
        sources = lake.read("Assessment_DataflowSources")
    by_df = defaultdict(list)
    for s in sources:
        by_df[s["dataflow_id"]].append(s)
    workspaces = {w["workspace_id"]: w for w in lake.read("Assessment_Workspaces")}
    updates = []
    for df in lake.read("Assessment_Dataflows"):
        srcs = by_df.get(df["dataflow_id"], [])
        scanned = df["dataflow_type"] == "Gen1" and (workspaces.get(df["workspace_id"]) or {}).get("scan_state") == "Scanned"
        if not srcs and not scanned:
            continue
        gateway_ids = {s["gateway_id"] for s in srcs if s.get("gateway_id")}
        updates.append({"dataflow_id": df["dataflow_id"], "source_count": len({s["source_key"] for s in srcs}),
                        "source_type_count": len({(s.get("datasource_type") or "").lower() for s in srcs}), "gateway_count": len(gateway_ids),
                        "uses_gateway": any(s.get("uses_gateway") for s in srcs), "uses_on_prem_gateway": any(s.get("is_on_premises") and s.get("uses_gateway") for s in srcs)})
    if updates:
        lake.merge("Assessment_Dataflows", updates)
    return {"rows_in": len(sources), "rows_out": len(updates), "message": f"{len(gateways)} gateways visible"}

# %% [markdown]
# ### Run - section 5: definitions, dependencies and connectivity

# %% tags=["run"]
run_steps(ctx, sections={5})
