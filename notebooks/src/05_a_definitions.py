# %% [markdown]
# # 5. Dependency collection
# ## 5.1 Definition analysis (Gen1 `model.json`, Gen2 `queryMetadata.json` + `mashup.pq`)
# Definitions are the only source for the *technical complexity* indicators and several migration rules. They are
# expensive (admin export: 200 req/h; Gen2 `getDefinition` needs read+write on the item), so collection is **budgeted per
# run, prioritized (new, then changed, then retries) and checkpointed by dataflow modification time**.
#
# The M analyzer is tokenizer-aware (strings, quoted identifiers and comments are masked before structure is derived),
# so a `;` or a keyword inside a string never splits a query. It extracts: queries, parameters, custom functions, linked
# entities (`PowerPlatform.Dataflows` / legacy `PowerBI.Dataflows` navigation with workspace and dataflow ids), computed
# entities (a loaded query referencing another loaded query), native queries and AI-insight functions.
# Incremental refresh is detected through the real CDM trait `pbi:refreshPolicy` (with `ppiConfig` as a tolerated alias).

# %%
import hashlib

_MASK_RE = re.compile(r'"(?:[^"]|"")*"|//[^\n]*|/\*.*?\*/', re.S)
_BRACKETS = re.compile(r"[()\[\]{};]")
_MEMBER_RE = re.compile(r'^\s*shared\s+(#"(?:[^"]|"")*"|[^\W\d][\w\.]*)\s*=', re.S)   # [^\W\d]: a letter (any script) or underscore
_PARAM_RE = re.compile(r"IsParameterQuery\s*=\s*true", re.I)
_NAV_RE = re.compile(r'\b(workspaceId|dataflowId|entity)\s*=\s*"([^"]+)"', re.I)
_NATIVE_RE = re.compile(r"Value\.NativeQuery\s*\(|\[\s*Query\s*=", re.I)
_AI_RE = re.compile(r"\b(?:AI|AzureML|CognitiveServices|AzureOpenAI|TextAnalytics|VisionService)\.[A-Za-z]\w*")
_SOURCE_CALL_RE = re.compile(r"\b([A-Z][A-Za-z0-9]*)\.([A-Z][A-Za-z0-9]*)\s*\(")
_DF_URL_RE = re.compile(r"groups/([0-9a-fA-F-]{36})/dataflows/([0-9a-fA-F-]{36})")
SOURCE_FUNCTION_ROOTS = {"Sql", "Oracle", "Web", "OData", "Excel", "Csv", "Json", "Xml", "SharePoint", "AzureStorage", "Odbc", "OleDb", "Folder",
                         "File", "Salesforce", "PostgreSQL", "MySQL", "Snowflake", "Lakehouse", "Warehouse", "AnalysisServices", "Teradata", "DB2",
                         "SapHana", "SapBusinessWarehouse", "GoogleAnalytics", "Dynamics365", "CommonDataService", "Dataverse", "Databricks",
                         "AzureDataExplorer", "Access", "Informix", "Sybase", "Redshift", "GoogleBigQuery", "Cds", "PowerPlatform", "PowerBI",
                         "AzureSQL", "AzureCosmos", "Cosmos", "MongoDB", "Parquet", "Pdf", "Exchange", "ActiveDirectory", "Kusto", "Fabric"}


def mask_m(text):
    """Replace string contents and comments with filler of equal length so structure can be derived safely."""
    def repl(match):
        piece = match.group(0)
        if piece.startswith('"'):
            return '"' + re.sub(r"[^\r\n]", "_", piece[1:-1]) + '"'
        return re.sub(r"[^\r\n]", " ", piece)
    return _MASK_RE.sub(repl, text)


def _top_level_segments(masked):
    depth, start, segments = 0, 0, []
    for match in _BRACKETS.finditer(masked):
        ch = match.group()
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth = max(0, depth - 1)
        elif depth == 0:
            segments.append((start, match.start()))
            start = match.end()
    if masked[start:].strip():
        segments.append((start, len(masked)))
    return segments


def _skip_attribute(masked, begin):
    """Skip leading [attribute] groups (e.g. [DataDestinations = ...]) of a segment."""
    pos = begin
    while True:
        while pos < len(masked) and masked[pos].isspace():
            pos += 1
        if pos >= len(masked) or masked[pos] != "[":
            return pos
        depth = 0
        for idx in range(pos, len(masked)):
            depth += masked[idx] == "["
            depth -= masked[idx] == "]"
            if depth == 0:
                pos = idx + 1
                break
        else:
            return pos


def is_function_expr(masked_expr):
    s = masked_expr.lstrip()
    if not s.startswith("("):
        return False
    depth = 0
    for idx, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                rest = s[idx + 1:].lstrip()
                return rest.startswith("=>") or bool(re.match(r"as\s+[\w\.\s\?]+?\s*=>", rest))
    return False


def parse_m_members(text, masked=None):
    masked = masked if masked is not None else mask_m(text)
    members = []
    for begin, end in _top_level_segments(masked):
        start = _skip_attribute(masked, begin)
        head = _MEMBER_RE.match(masked[start:end])
        if not head:
            continue
        name_span = head.span(1)
        raw_name = text[start + name_span[0]: start + name_span[1]]
        name = raw_name[2:-1].replace('""', '"') if raw_name.startswith('#"') else raw_name
        expr_start = start + head.end()
        expr, mexpr = text[expr_start:end], masked[expr_start:end]
        kind = "parameter" if _PARAM_RE.search(expr) else ("function" if is_function_expr(mexpr) else "query")
        members.append({"name": name, "kind": kind, "expr": expr, "masked": mexpr})
    return members


# Tokens of a masked M expression (string contents and comments are already blanked): quoted identifier, #keyword, string, number,
# dotted identifier (Table.SelectRows and dim.Customer are ONE identifier in M), =>, brackets, comma and equals; anything else is one character.
_TOKEN_RE = re.compile(r'(?P<quoted>#"[_\r\n]*")|(?P<hash>#[^\W\d]\w*)|(?P<string>"[_\r\n]*")|(?P<num>\d[\w\.]*)'
                       r'|(?P<name>[^\W\d]\w*(?:\.[^\W\d]\w*)*)|(?P<op>=>|[()\[\]{},=])|\S')
_CLOSER = {")": "paren", "}": "brace", "]": "bracket"}


class _LetScope:
    """A `let` being scanned: what it binds, what its initializers and body read, and the binding being initialised right now."""
    __slots__ = ("bound", "uses", "current", "body")

    def __init__(self):
        self.bound, self.uses, self.current, self.body = set(), set(), None, False


def _scan_m(expr, masked, interesting=None):
    """The names an M expression reads (a set), restricted to `interesting` when given. One pass, independent of the number of queries.

    * Field selectors ([Country], x[Country]?) and record keys ([Country = 1]) name fields, not queries, so they are not reads.
      Whether `Name =` is a binding or a comparison depends on the enclosing bracket, not on the preceding comma: it binds only
      right after a `let` or `[` or after a comma whose innermost enclosing scope is that `let` or `[`; inside (...) and {...}
      it is an equality test and the name is read.
    * `let` scopes are lexical. A step is visible in every initializer and in the body of its own `let`, whatever the order, but
      not in its own initializer (there the name reads the enclosing scope, unless written @name), and not outside the let: the
      body ends at the comma or closing bracket of whatever encloses the let. Names the let does not bind are passed outwards.
      Function parameters and the fields of a record literal are not modelled: they can only add a read that is not one."""
    tokens = [(m.lastgroup, m.start(), m.end()) for m in _TOKEN_RE.finditer(masked)]
    used, stack, lets, expect = set(), [], [], False

    def emit(name, innermost, inclusive=False):
        i = innermost
        while not inclusive and i >= 0 and lets[i].current == name:    # inside its own initializer: the name belongs to the enclosing scope
            i -= 1
        (lets[i].uses if i >= 0 else used).add(name)

    def pop_to(size):
        while len(stack) > size:
            frame = stack.pop()
            if isinstance(frame, _LetScope):
                lets.pop()
                for name in frame.uses - frame.bound:
                    emit(name, len(lets) - 1)

    for i, (kind, start, end) in enumerate(tokens):
        text = masked[start:end]
        if kind in ("name", "quoted"):
            if kind == "name" and text == "let":
                scope = _LetScope()
                stack.append(scope)
                lets.append(scope)
                expect = True
                continue
            if kind == "name" and text == "in":
                opened = next((j for j in range(len(stack) - 1, -1, -1) if isinstance(stack[j], _LetScope) and not stack[j].body), None)
                if opened is not None:
                    pop_to(opened + 1)                                  # brackets opened since the let and never closed are dropped
                    stack[opened].body, stack[opened].current = True, None
                    expect = False
                    continue
            following = masked[tokens[i + 1][1]:tokens[i + 1][2]] if i + 1 < len(tokens) else ""
            top = stack[-1] if stack else None
            binds, selects = expect and following == "=", expect and top == "bracket" and following == "]"
            expect = False
            name = text if kind == "name" else expr[start + 2:end - 1].replace('""', '"')
            if binds:
                if isinstance(top, _LetScope) and not top.body:
                    top.current = name
                    if interesting is None or name in interesting:
                        top.bound.add(name)
            elif not selects and (interesting is None or name in interesting):
                emit(name, len(lets) - 1, inclusive=i > 0 and masked[tokens[i - 1][1]:tokens[i - 1][2]] == "@")
        elif text == "[":
            stack.append("bracket")
            expect = True
        elif text == "(":
            stack.append("paren")
            expect = False
        elif text == "{":
            stack.append("brace")
            expect = False
        elif text in _CLOSER:
            for j in range(len(stack) - 1, -1, -1):
                if stack[j] == _CLOSER[text]:
                    pop_to(j)
                    break
            expect = False
        elif text == ",":
            while stack and isinstance(stack[-1], _LetScope) and stack[-1].body:    # the comma of what encloses the let ends its body
                pop_to(len(stack) - 1)
            top = stack[-1] if stack else None
            if isinstance(top, _LetScope):
                top.current = None
            expect = isinstance(top, _LetScope) or top == "bracket"
        else:
            expect = False
    pop_to(0)
    return used


def analyze_m_document(text):
    """Structure and risk signals of an M section document. Safe on empty or malformed input."""
    text = text or ""
    members = parse_m_members(text)
    queries = [m for m in members if m["kind"] == "query"]
    names = [m["name"] for m in queries]
    refs, linked, natives, ai, connectors = {}, [], [], set(), Counter()
    legacy = False
    name_set, order = set(names), {n: i for i, n in enumerate(names)}
    for member in queries:
        expr, mexpr = member["expr"], member["masked"]
        refs[member["name"]] = sorted(_scan_m(expr, mexpr, name_set) - {member["name"]}, key=order.get)
        if re.search(r"(?:PowerPlatform|PowerBI)\.Dataflows\s*\(", mexpr):
            is_legacy = bool(re.search(r"PowerBI\.Dataflows\s*\(", mexpr))
            legacy = legacy or is_legacy
            workspace, mine = None, []
            for key, value in _NAV_RE.findall(expr):
                key = key.lower()
                if key == "workspaceid":
                    workspace = value
                elif key == "dataflowid":
                    mine.append({"query": member["name"], "workspace_id": gid(workspace), "dataflow_id": gid(value), "entity": None, "legacy": is_legacy})
                elif mine:
                    mine[-1]["entity"] = value
            linked.extend(mine or [{"query": member["name"], "workspace_id": gid(workspace), "dataflow_id": None, "entity": None, "legacy": is_legacy}])
        if _NATIVE_RE.search(mexpr):
            natives.append(member["name"])
        ai.update(_AI_RE.findall(mexpr))
        for root, fn in _SOURCE_CALL_RE.findall(mexpr):
            if root in SOURCE_FUNCTION_ROOTS:
                connectors[f"{root}.{fn}"] += 1
    return {"query_names": names, "parameter_names": [m["name"] for m in members if m["kind"] == "parameter"],
            "function_names": [m["name"] for m in members if m["kind"] == "function"], "refs": refs, "linked": linked,
            "native_queries": natives, "ai_functions": sorted(ai), "connectors": dict(connectors), "legacy_connector": legacy, "chars": len(text)}

# %%
def _find_keys(obj, predicate, depth=0):
    """Yield (key, value) pairs anywhere in a JSON structure whose key satisfies predicate."""
    if depth > 8:
        return
    if isinstance(obj, dict):
        for key, value in obj.items():
            if predicate(str(key)):
                yield key, value
            yield from _find_keys(value, predicate, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            yield from _find_keys(item, predicate, depth + 1)


def entity_has_incremental_refresh(entity):
    """pbi:refreshPolicy of type IncrementalRefreshPolicy (CDM extension); ppiConfig tolerated as an alias."""
    for key, value in _find_keys(entity, lambda k: k.lower() in ("pbi:refreshpolicy", "refreshpolicy", "ppiconfig")):
        if key.lower() == "ppiconfig" and value:
            return True
        if isinstance(value, dict):
            if "incremental" in str(value.get("$type", "")).lower():
                return True
            if any(k in value for k in ("incrementalGranularity", "rollingWindowGranularity", "incrementalPeriods", "rollingWindowPeriods")):
                return True
    return False


def entity_is_directquery(entity):
    for key, value in _find_keys(entity, lambda k: k.lower() in ("mode", "storagemode")):
        if str(value).lower() == "directquery":
            return True
    return False


def _model_mashup(model):
    mashup = model.get("pbi:mashup")
    if not isinstance(mashup, dict):
        for annotation in model.get("annotations") or []:
            if isinstance(annotation, dict) and annotation.get("name") == "pbi:mashup":
                value = annotation.get("value")
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        value = None
                mashup = value if isinstance(value, dict) else None
    return mashup if isinstance(mashup, dict) else {}


def _definition_result(analysis, entity_names, enabled, incremental, directquery, extra_linked):
    """Shared shaping of facts (Silver columns) and evidence for Gen1 and Gen2 definitions."""
    loaded = set(enabled)
    computed = sorted(q for q in loaded if any(r in loaded for r in analysis["refs"].get(q, [])))
    linked = {l["query"] for l in analysis["linked"]} | set(extra_linked)
    findings = {"incremental_entities": sorted(incremental), "computed_entities": computed, "linked_entities": sorted(linked),
                "native_query_entities": sorted(analysis["native_queries"]), "directquery_entities": sorted(directquery),
                "ai_functions": analysis["ai_functions"]}
    facts = {
        "query_count": len(analysis["query_names"]) or len(entity_names), "entity_count": len(loaded) if loaded else len(entity_names),
        "custom_function_count": len(analysis["function_names"]), "parameter_count": len(analysis["parameter_names"]),
        "incremental_refresh_count": len(incremental), "computed_entity_count": len(computed), "linked_entity_count": len(linked),
        "directquery_entity_count": len(directquery), "native_query_count": len(analysis["native_queries"]), "m_script_chars": analysis["chars"],
        "legacy_connector_used": analysis["legacy_connector"], "ai_function_count": len(analysis["ai_functions"]),
        "definition_findings": json.dumps(findings, sort_keys=True),
    }
    return facts


def analyze_model_json(model):
    """Gen1 definition. Returns {'facts', 'linked_refs', 'hash'}."""
    model = model if isinstance(model, dict) else {}
    mashup = _model_mashup(model)
    document = mashup.get("document") or ""
    analysis = analyze_m_document(document)
    entities = [e for e in (model.get("entities") or []) if isinstance(e, dict)]
    entity_names = [str(e.get("name")) for e in entities if e.get("name")]
    qmeta = mashup.get("queriesMetadata") if isinstance(mashup.get("queriesMetadata"), dict) else {}
    if qmeta:
        known = set(analysis["query_names"])
        enabled = [n for n, meta in qmeta.items() if isinstance(meta, dict) and meta.get("loadEnabled") is not False]
        enabled = [n for n in enabled if n in known] or ([] if known else enabled)
    else:
        enabled = entity_names
    reference_models = {gid(r.get("id")): r.get("location") for r in (model.get("referenceModels") or []) if isinstance(r, dict)}
    ref_entities, linked_refs = [], [dict(l) for l in analysis["linked"] if l["dataflow_id"]]
    for entity in entities:
        if entity.get("$type") == "ReferenceEntity" or entity.get("linkedEntityRef"):
            ref_entities.append(str(entity.get("name")))
            location = reference_models.get(gid(entity.get("modelId"))) or ""
            match = _DF_URL_RE.search(str(location))
            if match:
                linked_refs.append({"query": entity.get("name"), "workspace_id": gid(match.group(1)), "dataflow_id": gid(match.group(2)),
                                    "entity": entity.get("source"), "legacy": False})
    incremental = [str(e.get("name")) for e in entities if entity_has_incremental_refresh(e)]
    directquery = [str(e.get("name")) for e in entities if entity_is_directquery(e)]
    facts = _definition_result(analysis, entity_names, enabled, incremental, directquery, ref_entities)
    seen, unique = set(), []
    for ref in linked_refs:
        key = (ref["dataflow_id"], ref["query"])
        if key not in seen:
            seen.add(key)
            unique.append(ref)
    digest = short_hash(document + "|" + ",".join(sorted(entity_names)) + "|" + json.dumps(model.get("referenceModels") or [], sort_keys=True, default=str), 20)
    return {"facts": facts, "linked_refs": unique, "hash": digest, "connections": [], "gateway_object_id": None}


def decode_parts(definition):
    parts = {}
    for part in (definition or {}).get("parts") or []:
        payload = part.get("payload")
        if payload is None or part.get("payloadType", "InlineBase64") != "InlineBase64":
            continue
        try:
            parts[part.get("path")] = base64.b64decode(payload).decode("utf-8", "replace")
        except (ValueError, TypeError):
            continue
    return parts


def analyze_gen2_definition(definition):
    """Gen2 definition: queryMetadata.json + mashup.pq, or the legacy dataflow-content.json (editingSessionMashup)."""
    parts = decode_parts(definition)
    meta, document = {}, parts.get("mashup.pq") or ""
    try:
        if "queryMetadata.json" in parts:
            meta = json.loads(parts["queryMetadata.json"])
        elif "dataflow-content.json" in parts:
            content = json.loads(parts["dataflow-content.json"])
            session = content.get("editingSessionMashup") or content
            document = document or session.get("mashupDocument") or ""
            meta = {"queriesMetadata": session.get("queriesMetadata") or {}, "connections": session.get("connectionOverrides") or [],
                    "gatewayObjectId": session.get("gatewayObjectId")}
    except ValueError:
        meta = {}
    analysis = analyze_m_document(document)
    qmeta = meta.get("queriesMetadata") if isinstance(meta.get("queriesMetadata"), dict) else {}
    enabled = [n for n, m in qmeta.items() if isinstance(m, dict) and m.get("loadEnabled", True) is not False]
    if not qmeta:
        enabled = analysis["query_names"]
    facts = _definition_result(analysis, list(qmeta) or analysis["query_names"], enabled, [], [], [])
    if meta.get("parametric") and not facts["parameter_count"]:
        facts["parameter_count"] = 1
    linked_refs = [dict(l) for l in analysis["linked"] if l["dataflow_id"]]
    connections = [c for c in (meta.get("connections") or []) if isinstance(c, dict)]
    return {"facts": facts, "linked_refs": linked_refs, "hash": short_hash(document + json.dumps(meta, sort_keys=True, default=str), 20),
            "connections": connections, "gateway_object_id": gid(meta.get("gatewayObjectId"))}

# %%
DF_DEF_COLS = ["query_count", "entity_count", "custom_function_count", "parameter_count", "incremental_refresh_count", "computed_entity_count",
               "linked_entity_count", "directquery_entity_count", "native_query_count", "legacy_connector_used", "ai_function_count",
               "definition_findings", "m_script_chars", "definition_status", "definition_collected_at", "definition_hash"]
_RANK = {"new": 0, "changed": 1, "retry": 2, "retry-failed": 2, "retry-denied": 3, "expired": 4}


def _version(row):
    moment = row.get("modified_at")
    return moment.isoformat() if moment else "none"


def select_candidates(ctx, phase, wanted, budget, ttl_hours=None, force=False):
    """Prioritized work list: never collected, then changed, then retries; denied / up-to-date entities are skipped."""
    scope = ctx.state.get("scope_ws_ids")
    picked = []
    for row in ctx.lake.read("Assessment_Dataflows", where={"workspace_id": scope} if scope else None):
        if not wanted(row):
            continue
        collect, reason = ctx.checkpoints.decide("Dataflow", row["dataflow_id"], phase, version=_version(row) if phase == "definition" else None,
                                                  ttl_hours=ttl_hours, force=force)
        if not collect:
            continue
        cp = ctx.checkpoints.get("Dataflow", row["dataflow_id"], phase)
        rank = 4 if (force and cp is not None and reason == "new") else _RANK.get(reason, 5)
        picked.append((rank, (cp or {}).get("completed_at") or dt.datetime.min, row["dataflow_id"], row))
    picked.sort(key=lambda p: p[:3])
    return [p[3] for p in picked[:budget]], len(picked)


GEN2_DEFINITION_PARTS = ("queryMetadata.json", "mashup.pq", "dataflow-content.json")
GEN1_MODEL_KEYS = ("entities", "pbi:mashup", "annotations")


class EmptyDefinition(ApiError):
    """The API answered, but the body holds no dataflow definition (empty or unexpected): not a result to store as collected."""


def fetch_definition(ctx, row):
    wid, did = row["workspace_id"], row["dataflow_id"]
    if row["dataflow_type"] == "Gen2":
        fabric = ctx.clients["fabric"]
        resp = fabric.request("POST", f"workspaces/{wid}/dataflows/{did}/getDefinition", json_body={}, ok=(200, 202), buckets=("fabric_platform",))
        body = fabric.lro(resp)
        definition = body.get("definition") or body
        if not any(part in decode_parts(definition) for part in GEN2_DEFINITION_PARTS):
            raise EmptyDefinition("the Gen2 definition holds none of " + ", ".join(GEN2_DEFINITION_PARTS), endpoint="dataflows/getDefinition", retryable=True)
        return analyze_gen2_definition(definition)
    pbi = ctx.clients["pbi"]
    order = ("workspace", "admin") if ctx.cfg.definition_source == "workspace" else ("admin", "workspace")
    denied = ctx.state.setdefault("definition_denied_workspaces", set())
    failure = None
    for source in order:
        if source == "workspace" and wid in denied:
            continue
        try:
            if source == "admin":
                model = pbi.get_json(f"admin/dataflows/{did}/export", buckets=("pbi_admin_dataflow_export",))
            else:
                model = pbi.get_json(f"groups/{wid}/dataflows/{did}", buckets=("pbi_dataflow",))
            if not isinstance(model, dict) or not any(key in model for key in GEN1_MODEL_KEYS):
                raise EmptyDefinition(f"the {source} API returned no dataflow model", endpoint="dataflows/definition", retryable=True)
            return analyze_model_json(model)
        except (ForbiddenError, NotFoundError, AuthError, EmptyDefinition) as exc:
            failure = exc
            if source == "workspace" and isinstance(exc, (ForbiddenError, AuthError)):
                denied.add(wid)
    raise failure


def _definition_edges(row, result):
    edges = []
    for ref in result["linked_refs"]:
        if ref["dataflow_id"] and ref["dataflow_id"] != row["dataflow_id"]:
            attrs = {"legacy_connector": True} if ref.get("legacy") else None
            edges.append(edge_row("Dataflow", ref["dataflow_id"], ref["workspace_id"], None, "Dataflow", row["dataflow_id"], row["workspace_id"],
                                  row.get("dataflow_name"), "LinkedEntity", evidence="Definition", attributes=attrs))
    return edges


def _connection_sources(row, result):
    sources = []
    for conn in result["connections"]:
        kind, path = conn.get("kind") or "Unknown", str(conn.get("path") or "")
        details = {"path": path}
        if kind.lower() in ("sql", "azuresqldatabase", "azuresql", "synapse", "postgresql", "mysql", "oracle", "snowflake"):
            server, _, database = path.partition(";")
            details = {"server": server, "database": database or None}
        elif path.lower().startswith("http"):
            details = {"url": path}
        gateway = result.get("gateway_object_id")
        inst = {"type": kind, "details": {k: v for k, v in details.items() if v}, "gateway_id": gateway}
        src = build_source_row(row["dataflow_id"], row["workspace_id"], gid(conn.get("connectionId")), inst, "Definition")
        src["connection_id"] = gid(conn.get("connectionId"))
        sources.append(src)
    return sources


def persist_definitions(ctx, results):
    """results: [(row, parsed result or None, status)] -> Silver rows, lineage edges and connection sources."""
    now, rows, edges, sources, ids = ctx.clock.now(), [], [], [], []
    for row, result, status in results:
        ids.append(row["dataflow_id"])
        update = {"dataflow_id": row["dataflow_id"], "definition_status": status, "definition_collected_at": now if status == "Collected" else None}
        if result:
            update.update(result["facts"])
            update["definition_hash"] = result["hash"]
            edges += _definition_edges(row, result)
            sources += _connection_sources(row, result)
        rows.append(update)
    collected = [r for r, res, st in results if st == "Collected"]
    if collected:
        ctx.lake.merge("Assessment_Dataflows", [r for r in rows if r.get("definition_status") == "Collected"], update_cols=DF_DEF_COLS)
    others = [r for r in rows if r.get("definition_status") != "Collected"]
    if others:
        ctx.lake.merge("Assessment_Dataflows", others, update_cols=["definition_status"])
    collected_ids = {c["dataflow_id"] for c in collected}
    new_edges = {e["edge_id"] for e in edges}
    stale = [{"edge_id": r["edge_id"]} for r in ctx.lake.read("Assessment_Lineage", where={"dst_id": collected_ids})
             if r.get("evidence") == "Definition" and r["edge_id"] not in new_edges] if collected_ids else []
    if stale:
        ctx.lake.mark_deleted("Assessment_Lineage", stale)
    if edges:
        ctx.lake.merge("Assessment_Lineage", edges)
    if sources:
        merge_by_signature(ctx.lake, "Assessment_DataflowSources", sources)
    return len(rows)


BREAKER_THRESHOLD = 25


def run_budgeted(ctx, phase, candidates, work, handle_ok, persist, entity_type="Dataflow", key_of=None, breaker=BREAKER_THRESHOLD):
    """Execute work(row) for candidates in a thread pool; checkpoint outcomes; stop gracefully on throttling / deadline.

    persist(outcomes) receives [(row, result | None, status)] in micro-batches of p_batch_flush_size.
    Circuit breaker: when the first `breaker` calls are all denied (401/403) the phase stops instead of burning the quota.
    """
    cfg, outcomes, stop, done = ctx.cfg, [], None, 0
    key_of = key_of or (lambda r: r["dataflow_id"])
    denied = succeeded = 0
    pending = deque(candidates)
    with cf.ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
        inflight = {}
        while pending or inflight:
            while pending and len(inflight) < cfg.max_workers * 2 and stop is None and not ctx.deadline.expired(reserve_sec=60):
                row = pending.popleft()
                inflight[pool.submit(work, row)] = row
            if not inflight:
                break
            finished, _ = cf.wait(list(inflight), return_when=cf.FIRST_COMPLETED)
            for future in finished:
                row = inflight.pop(future)
                key = key_of(row)
                try:
                    outcome = future.result()
                except GracefulStop as exc:
                    stop = exc
                    continue
                except (ForbiddenError, AuthError) as exc:
                    denied += 1
                    ctx.checkpoints.mark(entity_type, key, phase, "Denied", error=str(exc))
                    ctx.record_error(phase, entity_type, key, exc.endpoint, exc=exc)
                    outcomes.append((row, None, "AccessDenied"))
                except NotFoundError as exc:
                    ctx.checkpoints.mark(entity_type, key, phase, "NotFound", error=str(exc))
                    outcomes.append((row, None, "Failed"))
                except Exception as exc:  # noqa: BLE001 - per-entity isolation
                    ctx.checkpoints.mark(entity_type, key, phase, "Failed", error=f"{type(exc).__name__}: {exc}")
                    ctx.record_error(phase, entity_type, key, getattr(exc, "endpoint", None), exc=exc)
                    outcomes.append((row, None, "Failed"))
                else:
                    succeeded += 1
                    handle_ok(row, outcome)
                    outcomes.append((row, outcome, "Collected"))
                done += 1
                if breaker and succeeded == 0 and denied >= breaker and stop is None:
                    stop = AccessBreaker(f"the first {denied} calls were all denied (HTTP 401/403): grant the service principal access to the workspaces "
                                         f"(or use p_auth_mode = user as a Fabric admin) and re-run")
                if len(outcomes) >= cfg.batch_flush_size:
                    persist(outcomes)
                    outcomes = []
                    ctx.checkpoints.flush()
    if outcomes:
        persist(outcomes)
    ctx.checkpoints.flush()
    if stop is not None:
        raise stop
    return done


def phase_definitions(ctx):
    cfg = ctx.cfg
    wanted = lambda r: ((r["dataflow_type"] == "Gen1" and cfg.collect_gen1 and cfg.collect_definitions)  # noqa: E731
                        or (r["dataflow_type"] == "Gen2" and cfg.collect_gen2_definitions))
    candidates, eligible = select_candidates(ctx, "definition", wanted, cfg.budget_definitions_per_run, force=cfg.run_mode == "full")
    ctx.logger.info("definitions: %d eligible, %d selected (budget %d)", eligible, len(candidates), cfg.budget_definitions_per_run)

    def ok(row, result):
        ctx.checkpoints.mark("Dataflow", row["dataflow_id"], "definition", "Done", version=_version(row))

    done = run_budgeted(ctx, "definition", candidates, lambda row: fetch_definition(ctx, row), ok, lambda outcomes: persist_definitions(ctx, outcomes))
    denied = sum(1 for r in ctx.error_rows if r["phase"] == "definition" and r["http_status"] in (401, 403))
    if denied:
        ctx.logger.warning("%d definitions could not be read (HTTP 401/403): add the service principal to those workspaces (Contributor for Gen2) "
                           "or accept lower assessment confidence for them", denied)
    left = max(0, eligible - len(candidates))
    if left:
        ctx.logger.info("%d eligible dataflows remain for the next run", left)
    return {"rows_in": eligible, "rows_out": done, "status": "Partial" if left and done else "Succeeded",
            "message": f"{done} collected, {denied} denied, {left} left for the next run"}
