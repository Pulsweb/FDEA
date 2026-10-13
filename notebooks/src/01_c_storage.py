# %% [markdown]
# ## 1.4 Storage layer (Delta through Spark, files through the mounted Lakehouse)
# The pipeline only talks to the `Lake` contract below. `SparkLake` implements it with Delta `MERGE`;
# `InMemoryLake` implements the same semantics for dry runs and offline tests.
#
# * **Partial-column upserts** - each phase owns a set of columns of a table; `merge` only overwrites the columns the
#   phase supplies, so the refresh phase can never null out what the Scanner phase wrote.
# * **Soft deletes** for current-state tables (`is_deleted`, `deleted_at`) - history is never lost.
# * **Schema evolution** - missing registry columns are added to existing tables (`ALTER TABLE ... ADD COLUMNS`).
# * **Checkpoints** - `Assessment_Checkpoint` records what was collected for which entity version, driving incremental
#   runs, TTLs and retry back-off for denied or failing entities.

# %%
import copy


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def resolve_update_cols(table, rows, update_cols=None):
    """Columns a merge may overwrite: explicit list, else the columns present in the supplied rows."""
    reserved = set(table.keys) | {c[0] for c in _AUDIT[table.audit]}
    known = set(table.col_names)
    if update_cols:
        chosen = [c for c in update_cols if c in known]
    else:
        present = set()
        for row in rows:
            present.update(row)
        chosen = [c for c in table.col_names if c in present]
    return [c for c in chosen if c not in reserved]


def merge_sql(table, target, source, update_cols, prune=None):
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in table.keys)
    for column, values in (prune or {}).items():
        on += f" AND t.{column} IN ({', '.join(sql_literal(v) for v in sorted(values))})"
    sets = [f"t.{c} = s.{c}" for c in update_cols]
    if table.audit == "scd":
        sets += ["t.last_seen_at = s.last_seen_at", "t.is_deleted = false", "t.deleted_at = NULL", "t.run_id = s.run_id"]
    elif table.audit == "run":
        sets += ["t.run_id = s.run_id", "t.updated_at = s.updated_at"]
    cols = table.col_names
    action = f"WHEN MATCHED THEN UPDATE SET {', '.join(sets)} " if sets else ""
    return (f"MERGE INTO {target} AS t USING {source} AS s ON {on} {action}"
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})")


def soft_delete_sql(table, target, source):
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in table.keys)
    return (f"MERGE INTO {target} AS t USING {source} AS s ON {on} WHEN MATCHED AND (t.is_deleted = false OR t.is_deleted IS NULL) THEN "
            f"UPDATE SET t.is_deleted = true, t.deleted_at = s.deleted_at, t.run_id = s.run_id")


def dedupe_rows(table, rows):
    """Last row wins per key (MERGE rejects several source rows for one target row)."""
    latest = {}
    for row in rows:
        latest[tuple(row.get(k) for k in table.keys)] = row
    return list(latest.values())


def engine_conflict(exc):
    """The Delta / Fabric engine rejected a statement for an internal reason (an ambiguous internal column), not because of our data."""
    text = str(exc)
    return "AMBIGUOUS_REFERENCE" in text or "filePath" in text


def short_error(exc, limit=300):
    return " ".join(str(exc).split())[:limit]


def _matches(row, where):
    for column, wanted in (where or {}).items():
        value = row.get(column)
        if isinstance(wanted, (list, tuple, set, frozenset)):
            if value not in wanted:
                return False
        elif value != wanted:
            return False
    return True


class LakeBase:
    """Contract used by the pipeline. Rows are dicts keyed by registry column names."""

    def __init__(self, run_id, now=utcnow):
        self.run_id = run_id
        self.now = now

    def _stamp(self, table, row, insert):
        now = self.now()
        if table.audit == "scd":
            if insert:
                row["first_seen_at"] = now
            row.update(last_seen_at=now, is_deleted=False, deleted_at=None, run_id=self.run_id)
        elif table.audit == "run":
            row.update(run_id=self.run_id, updated_at=now)
        return row

    def ensure_tables(self):
        raise NotImplementedError

    def read(self, table, where=None, include_deleted=False):
        raise NotImplementedError

    def merge(self, table, rows, update_cols=None, prune=None):
        raise NotImplementedError

    def mark_deleted(self, table, keys):
        raise NotImplementedError

    def append(self, table, rows):
        raise NotImplementedError

    def replace(self, table, rows):
        raise NotImplementedError

    def count(self, table):
        raise NotImplementedError

    def read_external(self, name):
        """Rows of a table this notebook does not own (e.g. the Save-As accelerator results); None when it does not exist."""
        return None

    def maintain(self, table, vacuum_hours=0):
        """Compaction and cleanup. Returns the actions that really ran, or None when this lake has nothing to maintain."""
        return None


class InMemoryLake(LakeBase):
    def __init__(self, run_id="test-run", now=utcnow):
        super().__init__(run_id, now)
        self.data = {name: ({} if t.mode != "append" else []) for name, t in TABLES.items()}
        self.external = {}

    def read_external(self, name):
        rows = self.external.get(name)
        return None if rows is None else copy.deepcopy(rows)

    def ensure_tables(self):
        return None

    def _rows(self, name):
        store = self.data[name]
        return list(store.values()) if isinstance(store, dict) else list(store)

    def read(self, table, where=None, include_deleted=False):
        t = TABLES[table]
        out = []
        for row in self._rows(table):
            if t.soft_delete and row.get("is_deleted") and not include_deleted:
                continue
            if _matches(row, where):
                out.append(copy.deepcopy(row))
        return out

    def merge(self, table, rows, update_cols=None, prune=None):
        t = TABLES[table]
        rows = [dict(r) for r in rows]
        if not rows:
            return 0
        cols = resolve_update_cols(t, rows, update_cols)
        store = self.data[table]
        for coerced in dedupe_rows(t, [coerce_row(t, r) for r in rows]):
            key = tuple(coerced[k] for k in t.keys)
            existing = store.get(key)
            if existing is None:
                store[key] = self._stamp(t, dict(coerced), insert=True)
            else:
                for c in cols:
                    existing[c] = coerced.get(c)
                self._stamp(t, existing, insert=False)
        return len(rows)

    def mark_deleted(self, table, keys):
        t = TABLES[table]
        store = self.data[table]
        changed = 0
        for key_dict in keys:
            key = tuple(key_dict.get(k) for k in t.keys)
            row = store.get(key)
            if row is not None and not row.get("is_deleted"):
                row.update(is_deleted=True, deleted_at=self.now(), run_id=self.run_id)
                changed += 1
        return changed

    def append(self, table, rows):
        t = TABLES[table]
        rows = list(rows)
        for raw in rows:
            self.data[table].append(self._stamp(t, coerce_row(t, raw), insert=True))
        return len(rows)

    def replace(self, table, rows):
        t = TABLES[table]
        store = {}
        for raw in dedupe_rows(t, [coerce_row(t, r) for r in rows]):
            store[tuple(raw[k] for k in t.keys)] = self._stamp(t, raw, insert=True)
        self.data[table] = store if t.mode != "append" else list(store.values())
        return len(store)

    def count(self, table):
        return len(self._rows(table))


class SparkLake(LakeBase):
    def __init__(self, spark, schema, run_id, logger=None, now=utcnow):
        super().__init__(run_id, now)
        self.spark = spark
        self.schema = schema
        self.logger = logger or logging.getLogger("dea")
        for key, value in (("spark.sql.session.timeZone", "UTC"),
                           ("spark.microsoft.delta.optimizeWrite.enabled", "true"),
                           ("spark.sql.parquet.vorder.default", "true")):
            try:
                spark.conf.set(key, value)
            except Exception:  # noqa: BLE001 - optional Fabric tuning knobs
                self.logger.debug("could not set %s", key)

    def fqn(self, table):
        return qualified(self.schema, table)

    def _struct(self, table):
        from pyspark.sql import types as T
        mapping = {"STRING": T.StringType(), "BOOLEAN": T.BooleanType(), "INT": T.IntegerType(), "BIGINT": T.LongType(),
                   "DOUBLE": T.DoubleType(), "TIMESTAMP": T.TimestampType(), "DATE": T.DateType(),
                   "ARRAY<STRING>": T.ArrayType(T.StringType(), True)}
        return T.StructType([T.StructField(n, mapping[ctype], True) for n, ctype, _ in table.all_cols()])

    def _frame(self, table, rows):
        names = table.col_names
        return self.spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], schema=self._struct(table))

    def ensure_tables(self):
        if self.schema:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        for table in TABLES.values():
            self.spark.sql(ddl_for(table, self.schema))
            existing = {c.lower() for c in self.spark.table(self.fqn(table.name)).columns}
            missing = [(n, t, d) for n, t, d in table.all_cols() if n.lower() not in existing]
            if missing:
                cols = ", ".join(f"{n} {t} COMMENT '{_sql_comment(d)}'" for n, t, d in missing)
                self.spark.sql(f"ALTER TABLE {self.fqn(table.name)} ADD COLUMNS ({cols})")
                self.logger.info("schema evolution: added %s to %s", [m[0] for m in missing], table.name)

    def _read_frame(self, table, where=None, include_deleted=False):
        t = TABLES[table]
        df = self.spark.table(self.fqn(table))
        for column, wanted in (where or {}).items():
            if isinstance(wanted, (list, tuple, set, frozenset)):
                df = df.filter(df[column].isin(list(wanted)))
            else:
                df = df.filter(df[column] == wanted)
        if t.soft_delete and not include_deleted:
            df = df.filter("is_deleted = false OR is_deleted IS NULL")
        return [r.asDict(recursive=True) for r in df.collect()]

    def read(self, table, where=None, include_deleted=False):
        try:
            return self._read_frame(table, where, include_deleted)
        except Exception as exc:  # noqa: BLE001
            if not where or not engine_conflict(exc):
                raise self._annotated(exc, "reading", table)
            self.logger.warning("reading %s with a filter failed (%s): filtering in Python instead", table, short_error(exc))
            return [r for r in self._read_frame(table, None, include_deleted) if _matches(r, where)]

    def _annotated(self, exc, action, table):
        try:
            exc.add_note(f"while {action} {table}")
        except Exception:  # noqa: BLE001 - notes need Python 3.11
            pass
        self.logger.error("%s %s failed: %s: %s", action, table, type(exc).__name__, short_error(exc))
        return exc

    def merge(self, table, rows, update_cols=None, prune=None):
        t = TABLES[table]
        rows = [dict(r) for r in rows]
        if not rows:
            return 0
        cols = resolve_update_cols(t, rows, update_cols)
        staged = [self._stamp(t, r, insert=True) for r in dedupe_rows(t, [coerce_row(t, r) for r in rows])]
        view = f"_dea_stage_{t.name}"
        self._frame(t, staged).createOrReplaceTempView(view)
        statement = merge_sql(t, self.fqn(table), view, cols, prune)
        try:
            self.spark.sql(statement)
            return len(staged)
        except Exception as exc:  # noqa: BLE001
            if not engine_conflict(exc):
                raise self._annotated(exc, "merging into", table)
            self.logger.warning("MERGE into %s hit an engine conflict (%s)", table, short_error(exc))
        try:   # first retry: Fabric's low-shuffle merge is the usual suspect
            self.spark.conf.set("spark.microsoft.delta.merge.lowShuffle.enabled", "false")
            self.spark.sql(statement)
            self.logger.warning("MERGE into %s succeeded with the low-shuffle merge disabled for this session", table)
            return len(staged)
        except Exception as exc:  # noqa: BLE001
            if not engine_conflict(exc):
                raise self._annotated(exc, "merging into", table)
        self.logger.warning("MERGE into %s still fails: replacing the rows by key instead (delete, then insert)", table)
        self._replace_by_key(t, staged, cols)
        return len(staged)

    def _replace_by_key(self, t, staged, cols, chunk=200):
        """Same result as MERGE without the MERGE engine: the matching rows are read, updated in Python, deleted by key and inserted again.
        Not atomic, but idempotent: a crash in between is repaired by the next run, which writes the same rows again."""
        target, keys = self.fqn(t.name), list(t.keys)
        wanted = {tuple(r.get(k) for k in keys): r for r in staged}
        first, existing = keys[0], {}
        values = sorted({r.get(first) for r in staged if r.get(first) is not None})
        for i in range(0, len(values), 500):
            frame = self.spark.table(target)
            for row in frame.filter(frame[first].isin(values[i:i + 500])).collect():
                data = row.asDict(recursive=True)
                key = tuple(data.get(k) for k in keys)
                if key in wanted:
                    existing[key] = data
        final = []
        for key, new in wanted.items():
            old = existing.get(key)
            if old is None:
                final.append(new)
                continue
            merged = {c: old.get(c) for c in t.col_names}
            merged.update({c: new.get(c) for c in cols})
            for audit in ("last_seen_at", "is_deleted", "deleted_at", "run_id", "updated_at"):
                if audit in new and (t.audit == "scd" or audit in ("run_id", "updated_at")):
                    merged[audit] = new[audit]
            final.append(merged)
        for i in range(0, len(final), chunk):
            conditions = []
            for row in final[i:i + chunk]:
                parts = [f"{k} IS NULL" if row.get(k) is None else f"{k} = {sql_literal(row[k])}" for k in keys]
                conditions.append("(" + " AND ".join(parts) + ")")
            self.spark.sql(f"DELETE FROM {target} WHERE " + " OR ".join(conditions))
        view = f"_dea_fb_{t.name}"
        self._frame(t, final).createOrReplaceTempView(view)
        names = ", ".join(t.col_names)
        self.spark.sql(f"INSERT INTO {target} ({names}) SELECT {names} FROM {view}")

    def mark_deleted(self, table, keys):
        t = TABLES[table]
        keys = list(keys)
        if not keys:
            return 0
        from pyspark.sql import types as T
        now = self.now()
        names = list(t.keys) + ["is_deleted", "deleted_at", "run_id"]
        base = {f.name: f for f in self._struct(t).fields}
        rows = [tuple([kd.get(k) for k in t.keys] + [True, now, self.run_id]) for kd in keys]
        view = f"_dea_del_{t.name}"
        self.spark.createDataFrame(rows, schema=T.StructType([base[n] for n in names])).createOrReplaceTempView(view)
        self.spark.sql(soft_delete_sql(t, self.fqn(table), view))
        return len(rows)

    def append(self, table, rows):
        t = TABLES[table]
        rows = [self._stamp(t, coerce_row(t, r), insert=True) for r in rows]
        if not rows:
            return 0
        view = f"_dea_app_{t.name}"
        self._frame(t, rows).createOrReplaceTempView(view)
        self.spark.sql(f"INSERT INTO {self.fqn(table)} ({', '.join(t.col_names)}) SELECT {', '.join(t.col_names)} FROM {view}")
        return len(rows)

    def replace(self, table, rows):
        t = TABLES[table]
        rows = [self._stamp(t, r, insert=True) for r in dedupe_rows(t, [coerce_row(t, r) for r in rows])]
        if not rows:
            self.spark.sql(f"DELETE FROM {self.fqn(table)}")
            return 0
        view = f"_dea_rep_{t.name}"
        self._frame(t, rows).createOrReplaceTempView(view)
        # INSERT OVERWRITE maps by position: follow the physical column order (it differs from the registry after schema evolution)
        physical = self.spark.table(self.fqn(table)).columns
        known = set(t.col_names)
        select = ", ".join(c if c in known else f"NULL AS {c}" for c in physical)
        self.spark.sql(f"INSERT OVERWRITE TABLE {self.fqn(table)} SELECT {select} FROM {view}")
        return len(rows)

    def count(self, table):
        return self.spark.table(self.fqn(table)).count()

    def read_external(self, name):
        target = self.fqn(name)
        try:
            if not self.spark.catalog.tableExists(target):
                return None
            return [r.asDict(recursive=True) for r in self.spark.table(target).collect()]
        except Exception as exc:  # noqa: BLE001 - an unreadable optional table must not fail the analysis
            self.logger.warning("could not read %s: %s", target, exc)
            return None

    def maintain(self, table, vacuum_hours=0):
        """OPTIMIZE (Z-ORDER where defined) and, with a safe retention, VACUUM. Returns the actions that really ran."""
        t = TABLES[table]
        target = self.fqn(table)
        done = []
        try:
            zorder = f" ZORDER BY ({', '.join(t.zorder_by)})" if t.zorder_by else ""
            self.spark.sql(f"OPTIMIZE {target}{zorder}")
            done.append("OPTIMIZE")
            if vacuum_hours and vacuum_hours >= MIN_VACUUM_HOURS:
                self.spark.sql(f"VACUUM {target} RETAIN {int(vacuum_hours)} HOURS")
                done.append("VACUUM")
        except Exception as exc:  # noqa: BLE001 - maintenance must never fail the run
            self.logger.warning("maintenance skipped for %s: %s", table, exc)
        return done

# %%
class FileStore:
    """Files under <lakehouse>/Files/<root>. Paths are confined to the root (ids end up in file names)."""

    def __init__(self, root_dir, base_dir=None):
        self.root = os.path.abspath(root_dir)
        self.base = os.path.abspath(base_dir) if base_dir else self.root

    @classmethod
    def for_lakehouse(cls, files_root, base="/lakehouse/default", fallback=None):
        base_dir = base if os.path.isdir(base) else (fallback or os.path.join(os.getcwd(), "_lakehouse_default"))
        return cls(os.path.join(base_dir, *safe_files_root(files_root).split("/")), base_dir)

    def path(self, *parts):
        candidate = os.path.abspath(os.path.join(self.root, *[str(p) for p in parts]))
        if os.path.commonpath([self.root, candidate]) != self.root:
            raise ValueError(f"path escapes the file store root: {candidate}")
        return candidate

    def lakehouse_path(self, rel):
        """Resolve a path relative to the Lakehouse root (e.g. Files/dea/config/x.csv), confined to it."""
        candidate = os.path.abspath(os.path.join(self.base, rel.strip("/\\")))
        if os.path.commonpath([self.base, candidate]) != self.base:
            raise ValueError(f"path escapes the lakehouse root: {candidate}")
        return candidate

    def write_bytes(self, rel, data):
        full = self.path(rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as handle:
            handle.write(data)
        return full

    def write_json(self, rel, obj, compress=True):
        payload = json.dumps(obj, default=str, ensure_ascii=False).encode("utf-8")
        return self.write_bytes(rel, gzip.compress(payload) if compress else payload)

    def write_text(self, rel, text):
        return self.write_bytes(rel, text.encode("utf-8"))

    def read_bytes(self, rel):
        with open(self.path(rel), "rb") as handle:
            return handle.read()

    def read_json(self, rel):
        data = self.read_bytes(rel)
        if rel.endswith(".gz"):
            data = gzip.decompress(data)
        return json.loads(data.decode("utf-8"))

    def exists(self, rel):
        return os.path.exists(self.path(rel))

    def list(self, rel_dir=""):
        base = self.path(rel_dir) if rel_dir else self.root
        if not os.path.isdir(base):
            return []
        return sorted(os.path.join(rel_dir, name) if rel_dir else name for name in os.listdir(base))

    def purge_older_than(self, rel_dir, days, now=None):
        base = self.path(rel_dir)
        if not os.path.isdir(base):
            return 0
        cutoff = (now or time.time()) - days * 86400
        removed = 0
        for dirpath, _, files in os.walk(base):
            for name in files:
                full = os.path.join(dirpath, name)
                if os.path.getmtime(full) < cutoff:
                    os.remove(full)
                    removed += 1
        return removed

# %%
class CheckpointStore:
    """Incremental / restart state kept in Assessment_Checkpoint (one row per entity and phase)."""

    def __init__(self, lake, cfg, clock):
        self.lake = lake
        self.cfg = cfg
        self.clock = clock
        self._rows = {}
        self._dirty = {}
        self._lock = threading.Lock()

    def load(self):
        for row in self.lake.read("Assessment_Checkpoint"):
            self._rows[(row["entity_type"], row["entity_id"], row["phase"])] = row
        return len(self._rows)

    def get(self, entity_type, entity_id, phase):
        return self._rows.get((entity_type, entity_id, phase))

    def decide(self, entity_type, entity_id, phase, version=None, ttl_hours=None, force=False):
        """Return (collect: bool, reason: str) for an entity."""
        row = self.get(entity_type, entity_id, phase)
        if force or row is None:
            return True, "new"
        now = self.clock.now()
        status = row.get("status")
        attempted = row.get("last_attempt_at") or dt.datetime.min
        if status == "Done":
            if version is not None and row.get("source_version") != version:
                return True, "changed"
            completed = row.get("completed_at")
            if ttl_hours is not None and (completed is None or now - completed >= dt.timedelta(hours=ttl_hours)):
                return True, "expired"
            return False, "up-to-date"
        if status in ("Denied", "NotFound"):
            return (now - attempted >= dt.timedelta(days=self.cfg.denied_retry_days)), "retry-denied"
        if (row.get("attempts") or 0) >= self.cfg.max_attempts_per_entity:
            return (now - attempted >= dt.timedelta(days=self.cfg.failed_retry_days)), "retry-failed"
        return True, "retry"

    def mark(self, entity_type, entity_id, phase, status, version=None, error=None):
        """A version is kept for Done; for another status only when it is given explicitly (a restart records what it restarts for)."""
        now = self.clock.now()
        with self._lock:
            prev = self._rows.get((entity_type, entity_id, phase)) or {}
            keep_version = status == "Done" or version is not None
            row = {
                "entity_type": entity_type, "entity_id": entity_id, "phase": phase, "status": status,
                "attempts": 1 if status == "Done" else (prev.get("attempts") or 0) + 1,
                "last_attempt_at": now,
                "completed_at": now if status == "Done" else prev.get("completed_at"),
                "source_version": version if keep_version else prev.get("source_version"),
                "last_error": None if status == "Done" else truncate(error, 500),
            }
            self._rows[(entity_type, entity_id, phase)] = row
            self._dirty[(entity_type, entity_id, phase)] = row

    def flush(self):
        with self._lock:
            rows, self._dirty = list(self._dirty.values()), {}
        if not rows:
            return 0
        try:
            self.lake.merge("Assessment_Checkpoint", rows)
        except Exception:
            with self._lock:
                for row in rows:   # a row marked again meanwhile is newer: keep it, restore the others for the next flush
                    self._dirty.setdefault((row["entity_type"], row["entity_id"], row["phase"]), row)
            raise
        return len(rows)
