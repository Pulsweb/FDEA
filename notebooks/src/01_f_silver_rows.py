# %% [markdown]
# ## 1.7 Silver row builders
# Pure functions shared by the discovery sections (3 and 4) and the collectors: they turn Scanner payloads into
# `Assessment_Dataflows`, `Assessment_DataflowSources` and `Assessment_Lineage` rows, and persist them with the
# partial-column merge of the storage layer. They live in Section 1 because the Scanner phase (Section 3) and the later
# collectors all depend on them.

# %%
import urllib.parse

DF_SCAN_COLS = ["dataflow_key", "workspace_id", "dataflow_name", "dataflow_type", "generation", "description", "tags", "owner_upn",
                "owner_source", "modified_by", "modified_at", "endorsement", "certified_by", "sensitivity_label_id", "schedule_enabled",
                "schedule_days", "schedule_times", "schedule_runs_per_day", "schedule_timezone", "schedule_notify_option",
                "first_seen_dataflow_type", "type_changed_at"]
WS_SCAN_COLS = ["admin_upns", "admin_count", "gen1_dataflow_count", "gen2_dataflow_count", "dataset_count", "report_count",
                "dashboard_count", "other_item_count", "scanned_at", "scan_state"]
_KNOWN_ITEM_ARRAYS = {"reports", "dashboards", "datasets", "dataflows", "datamarts", "users", "tags"}


def merge_by_signature(lake, table, rows, **kw):
    """Merge rows grouped by the set of columns they supply, so a missing column never nulls existing data."""
    groups = defaultdict(list)
    for row in rows:
        groups[frozenset(row)].append(row)
    return sum(lake.merge(table, group, **kw) for group in groups.values())


def host_of(details):
    server = str(details.get("server") or "")
    if server:
        return server.split(",")[0].split(":")[0].split("\\")[0].strip().lower() or None
    url = str(details.get("url") or details.get("sharePointSiteUrl") or "")
    if url:
        return (urllib.parse.urlparse(url if "//" in url else "//" + url).hostname or "").lower() or None
    path = str(details.get("path") or "").replace("\\", "/").strip("/")
    return path.split("/")[0].lower() or None if path else None


def source_key_for(dtype, details):
    parts = [str(dtype or "").lower()] + [str(details.get(k) or "").lower() for k in ("server", "database", "url", "path", "sharePointSiteUrl")]
    return short_hash("|".join(parts))


def storage_type_of(model_url):
    if not model_url:
        return "Unknown"
    host = (urllib.parse.urlparse(str(model_url)).hostname or "").lower()
    return "CustomADLS" if host.endswith(".dfs.core.windows.net") else "PowerBIManaged"


def build_source_row(dataflow_id, workspace_id, datasource_id, inst, via, misconfigured=False):
    inst = inst or {}
    details = inst.get("details") or {}
    dtype = inst.get("type") or "Unknown"
    gateway = inst.get("gateway_id")
    category = categorize_source(dtype)
    text = json.dumps(details, sort_keys=True, default=str)
    local_path = category in FILE_BASED_CATEGORIES and bool(re.match(r"^([a-zA-Z]:\\|\\\\)", str(details.get("path") or "")))
    return {
        "dataflow_id": dataflow_id, "workspace_id": workspace_id, "source_key": source_key_for(dtype, details) if details else short_hash(f"id|{datasource_id}"),
        "datasource_id": datasource_id, "datasource_type": dtype, "source_category": category, "server": details.get("server"),
        "database": details.get("database"), "url": details.get("url") or details.get("sharePointSiteUrl"), "path": details.get("path"),
        "host": host_of(details), "connection_details": text if details else None, "gateway_id": gateway, "gateway_type": "Unknown" if gateway else None,
        "uses_gateway": bool(gateway), "is_on_premises": bool(gateway) or local_path, "is_misconfigured": bool(misconfigured or inst.get("misconfigured")),
        "discovered_via": via,
    }


def edge_row(src_type, src_id, src_ws, src_name, dst_type, dst_id, dst_ws, dst_name, relationship, kind="DATA", evidence="Scanner", attributes=None):
    src_id, dst_id = gid(src_id), gid(dst_id)
    return {"edge_id": short_hash(f"{src_id}|{dst_id}|{relationship}|{evidence}"), "src_type": src_type, "src_id": src_id, "src_workspace_id": gid(src_ws),
            "src_name": src_name, "dst_type": dst_type, "dst_id": dst_id, "dst_workspace_id": gid(dst_ws), "dst_name": dst_name,
            "relationship": relationship, "edge_kind": kind, "evidence": evidence,
            "attributes": json.dumps(attributes, sort_keys=True) if attributes else None}


def _schedule_columns(schedule):
    if not isinstance(schedule, dict):
        return {}
    times = [str(t) for t in (schedule.get("times") or [])]
    return {"schedule_enabled": _as_bool(schedule.get("enabled")) if schedule.get("enabled") is not None else None,
            "schedule_days": [str(d) for d in (schedule.get("days") or [])], "schedule_times": times, "schedule_runs_per_day": len(times),
            "schedule_timezone": schedule.get("localTimeZoneId"), "schedule_notify_option": schedule.get("notifyOption")}


def _endorsement(details):
    if isinstance(details, dict):
        return details.get("endorsement"), details.get("certifiedBy")
    return None, None


def _generation(d):
    return coerce_value("INT", d.get("generation"))


def parse_scan_result(ctx, result, batch_ids=()):
    """Normalize one Scanner result into Silver rows. Tolerant of undocumented / missing fields."""
    now = ctx.clock.now()
    instances = {}
    for key in ("datasourceInstances", "misconfiguredDatasourceInstances"):
        for inst in result.get(key) or []:
            did = gid(inst.get("datasourceId"))
            if did:
                instances[did] = {"type": inst.get("datasourceType"), "details": inst.get("connectionDetails") or {}, "gateway_id": gid(inst.get("gatewayId")),
                                  "misconfigured": key.startswith("misconfigured") or instances.get(did, {}).get("misconfigured", False)}
    out = {"workspaces": [], "dataflows": [], "sources": [], "edges": [], "scanned_workspace_ids": [], "missing_workspace_ids": [], "dataflow_ids_by_workspace": {}}
    for ws in result.get("workspaces") or []:
        wid = gid(ws.get("id"))
        if not wid:
            continue
        dataflows = [d for d in (ws.get("dataflows") or []) if gid(d.get("objectId") or d.get("id"))]
        admins = [str(u.get("emailAddress") or u.get("identifier") or u.get("displayName") or "") for u in (ws.get("users") or [])
                  if str(u.get("groupUserAccessRight") or "").lower() == "admin"]
        others = sum(len(v) for k, v in ws.items() if k not in _KNOWN_ITEM_ARRAYS and isinstance(v, list) and v and isinstance(v[0], dict))
        out["workspaces"].append({
            "workspace_id": wid, "admin_upns": [a for a in admins if a], "admin_count": len([a for a in admins if a]),
            "gen1_dataflow_count": sum(1 for d in dataflows if _generation(d) != 2), "gen2_dataflow_count": sum(1 for d in dataflows if _generation(d) == 2),
            "dataset_count": len(ws.get("datasets") or []), "report_count": len(ws.get("reports") or []),
            "dashboard_count": len(ws.get("dashboards") or []), "other_item_count": others, "scanned_at": now, "scan_state": "Scanned"})
        out["scanned_workspace_ids"].append(wid)
        ids_here = out["dataflow_ids_by_workspace"].setdefault(wid, set())
        for d in dataflows:
            did = gid(d.get("objectId") or d.get("id"))
            if not did:
                continue
            ids_here.add(did)
            generation = _generation(d)
            endorsement, certified_by = _endorsement(d.get("endorsementDetails"))
            out["dataflows"].append({
                "dataflow_key": stable_hash64(did), "dataflow_id": did, "workspace_id": wid, "dataflow_name": d.get("name"),
                "dataflow_type": "Gen2" if generation == 2 else "Gen1", "generation": generation or 1, "description": d.get("description"),
                "tags": [t.get("displayName") if isinstance(t, dict) else str(t) for t in (d.get("tags") or [])],
                "owner_upn": d.get("configuredBy"), "owner_source": "configuredBy" if d.get("configuredBy") else None,
                "modified_by": d.get("modifiedBy"), "modified_at": parse_ts(d.get("modifiedDateTime")), "endorsement": endorsement or "None",
                "certified_by": certified_by, "sensitivity_label_id": dig(d, "sensitivityLabel", "labelId"), **_schedule_columns(d.get("refreshSchedule"))})
            seen_sources = set()
            for usages, misconfigured in ((d.get("datasourceUsages"), False), (d.get("misconfiguredDatasourceUsages"), True)):
                for usage in usages or []:
                    sid = gid(usage.get("datasourceInstanceId"))
                    if sid and (sid, misconfigured) not in seen_sources:
                        seen_sources.add((sid, misconfigured))
                        row = build_source_row(did, wid, sid, instances.get(sid), "Scanner", misconfigured)
                        out["sources"].append(row)
                        details = (instances.get(sid) or {}).get("details") or {}
                        label = f"{(instances.get(sid) or {}).get('type') or 'Datasource'}: {details.get('server') or details.get('url') or details.get('path') or sid}"
                        out["edges"].append(edge_row("Datasource", sid, wid, label, "Dataflow", did, wid, d.get("name"), "DatasourceUsage"))
            for up in d.get("upstreamDataflows") or []:
                out["edges"].append(edge_row("Dataflow", up.get("targetDataflowId"), up.get("groupId"), None, "Dataflow", did, wid, d.get("name"), "LinkedEntity"))
        for ds in ws.get("datasets") or []:
            dsid = gid(ds.get("id"))
            if not dsid:
                continue
            text = json.dumps([ds.get("expressions"), ds.get("tables")], default=str).lower()
            attrs = {"storage_mode": ds.get("targetStorageMode"), "legacy_connector": "powerbi.dataflows(" in text}
            attrs = {k: v for k, v in attrs.items() if v}
            for up in ds.get("upstreamDataflows") or []:
                out["edges"].append(edge_row("Dataflow", up.get("targetDataflowId"), up.get("groupId"), None, "SemanticModel", dsid, wid, ds.get("name"),
                                             "DatasetSource", attributes=attrs or None))
        for rep in ws.get("reports") or []:
            if rep.get("datasetId") and rep.get("id"):
                out["edges"].append(edge_row("SemanticModel", rep["datasetId"], wid, None, "Report", rep["id"], wid, rep.get("name"), "ReportOnDataset"))
        for dash in ws.get("dashboards") or []:
            did_ = gid(dash.get("id"))
            for tile in dash.get("tiles") or []:
                for kind, key in (("SemanticModel", "datasetId"), ("Report", "reportId")):
                    if tile.get(key) and did_:
                        out["edges"].append(edge_row(kind, tile[key], wid, None, "Dashboard", did_, wid, dash.get("displayName") or dash.get("name"), "DashboardTile"))
        for mart in ws.get("datamarts") or []:
            for up in mart.get("upstreamDataflows") or mart.get("UpstreamDataflows") or []:
                out["edges"].append(edge_row("Dataflow", up.get("targetDataflowId"), up.get("groupId"), None, "Datamart", mart.get("id"), wid, mart.get("name"), "DatamartSource"))
    unique = {e["edge_id"]: e for e in out["edges"] if e["src_id"] and e["dst_id"]}
    out["edges"] = list(unique.values())
    requested = {gid(w) for w in batch_ids or ()} - {None}
    out["missing_workspace_ids"] = sorted(requested - set(out["scanned_workspace_ids"]))   # requested, but the Scanner did not return them
    return out


def persist_scan(ctx, parsed):
    lake, scanned = ctx.lake, set(parsed["scanned_workspace_ids"])
    existing = {r["dataflow_id"]: r for r in lake.read("Assessment_Dataflows", where={"workspace_id": scanned})} if scanned else {}
    for row in parsed["dataflows"]:
        prev = existing.get(row["dataflow_id"]) or {}
        row["first_seen_dataflow_type"] = prev.get("first_seen_dataflow_type") or row["dataflow_type"]
        changed = prev.get("dataflow_type") not in (None, row["dataflow_type"])
        row["type_changed_at"] = ctx.clock.now() if changed else prev.get("type_changed_at")
    merge_by_signature(lake, "Assessment_Dataflows", parsed["dataflows"], update_cols=DF_SCAN_COLS)
    lake.merge("Assessment_Workspaces", parsed["workspaces"], update_cols=WS_SCAN_COLS)

    new_sources = {(r["dataflow_id"], r["source_key"]) for r in parsed["sources"]}
    stale = [{"dataflow_id": r["dataflow_id"], "source_key": r["source_key"]} for r in lake.read("Assessment_DataflowSources", where={"workspace_id": scanned})
             if r.get("discovered_via") == "Scanner" and (r["dataflow_id"], r["source_key"]) not in new_sources] if scanned else []
    if stale:
        lake.mark_deleted("Assessment_DataflowSources", stale)
    merge_by_signature(lake, "Assessment_DataflowSources", parsed["sources"])

    new_edges = {e["edge_id"] for e in parsed["edges"]}
    stale_edges = [{"edge_id": r["edge_id"]} for r in lake.read("Assessment_Lineage", where={"dst_workspace_id": scanned})
                   if r.get("evidence") == "Scanner" and r["edge_id"] not in new_edges] if scanned else []
    if stale_edges:
        lake.mark_deleted("Assessment_Lineage", stale_edges)
    lake.merge("Assessment_Lineage", parsed["edges"])

    disc = ctx.state.setdefault("discovery", {"scanner": {}, "admin_pbi": {}, "admin_items": {}, "complete": {}})
    for row in parsed["dataflows"]:
        disc["scanner"][row["dataflow_id"]] = {"workspace_id": row["workspace_id"], "generation": row["generation"]}
