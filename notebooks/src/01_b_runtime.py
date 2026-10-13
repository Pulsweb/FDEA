# %% [markdown]
# ## 1.2 Constants, configuration object and error model

# %%
import collections
import concurrent.futures as cf
import contextlib
import csv
import datetime as dt
import gzip
import io
import itertools
import json
import logging
import math
import os
import random
import re
import statistics
import sys
import threading
import time
import traceback
import uuid
from collections import Counter, defaultdict, deque

import requests

NOTEBOOK_VERSION = "1.0.0"

PBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

RUN_MODES = ("full", "incremental", "resume", "analysis_only", "refresh_only")
AUTH_MODES = ("auto", "spn", "user")
PREFERRED_PATHS = ("auto", "wizard", "saveas")
DEFINITION_SOURCES = ("admin", "workspace")
MIN_VACUUM_HOURS = 168      # Delta refuses a shorter VACUUM retention unless the safety check is disabled
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

_PARAM_KINDS = {
    "str": ["run_mode", "resume_run_id", "log_level", "lakehouse_schema", "files_root", "auth_mode", "key_vault_url",
            "kv_secret_tenant_id", "kv_secret_client_id", "kv_secret_client_secret", "authority_host", "pbi_api_base",
            "fabric_api_base", "graph_api_base", "source_capacity", "preferred_path", "governance_overrides_path",
            "migration_results_table", "definition_source"],
    "bool": ["dry_run", "fail_on_partial", "persist_raw", "run_maintenance", "include_personal_workspaces",
             "include_inactive_workspaces", "collect_gen1", "collect_gen2", "collect_definitions", "collect_gen2_definitions",
             "collect_compute_engine", "collect_refresh_history", "collect_admin_datasources", "verify_upstream_admin_api",
             "collect_pipeline_references", "collect_item_connections", "enrich_owners_graph", "scan_get_users",
             "scan_dataset_expressions", "wizard_available", "require_definition_for_ready", "export_reports", "render_lineage_graph"],
    "int": ["max_runtime_minutes", "raw_retention_days", "vacuum_retention_hours", "scan_batch_size", "scan_max_concurrent",
            "scan_poll_interval_sec", "scan_timeout_sec", "scan_lookback_days", "max_workers", "max_retries",
            "max_retry_after_sec", "budget_definitions_per_run", "budget_refresh_dataflows_per_run", "budget_datasources_per_run",
            "refresh_history_ttl_hours", "refresh_history_max_records", "denied_retry_days", "failed_retry_days",
            "max_attempts_per_entity", "batch_flush_size", "stale_days", "failing_consecutive", "pilot_size", "top_n",
            "lineage_graph_max_nodes"],
    "float": ["rate_limit_scale", "retry_base_delay_sec", "retry_max_delay_sec"],
    "list": ["only_workspaces", "exclude_workspaces"],
    "dict": ["rate_limit_overrides", "score_weights", "environment_patterns"],
}


class DeaError(Exception):
    """Base class for all notebook errors."""


class ConfigError(DeaError):
    pass


class GracefulStop(DeaError):
    """Stops a phase early without failing the run (deadline, budget, throttling): resume on the next run."""


class BudgetExceeded(GracefulStop):
    pass


class DeadlineExceeded(GracefulStop):
    pass


class AccessBreaker(GracefulStop):
    """Raised when every early call is denied: stops burning API quota on a permission problem."""


class ApiError(DeaError):
    def __init__(self, message, status=None, code=None, url=None, body=None, retryable=False, endpoint=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.url = url
        self.body = body
        self.retryable = retryable
        self.endpoint = endpoint


class AuthError(ApiError):
    pass


class ForbiddenError(ApiError):
    pass


class NotFoundError(ApiError):
    pass


class ThrottleExhausted(ApiError, GracefulStop):
    pass


def _as_bool_param(value):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "y"):
        return True
    if text in ("false", "0", "no", "n", ""):
        return False
    raise ConfigError(f"not a boolean: {value!r}")


def as_list(value):
    """Accept lists, JSON arrays or comma / semicolon / newline separated text."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            return [str(v).strip() for v in json.loads(text) if str(v).strip()]
        except ValueError:
            pass
    return [p.strip() for p in re.split(r"[,;\n]", text) if p.strip()]


def as_dict(value):
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value))
    except ValueError as exc:
        raise ConfigError(f"not valid JSON: {value!r}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"expected a JSON object: {value!r}")
    return parsed


def safe_files_root(value):
    """p_files_root must be a folder below the Lakehouse Files area (for example Files/dea): no drive, no absolute path, no '..'."""
    parts = [p for p in str(value or "").replace("\\", "/").strip().split("/") if p not in ("", ".")]
    if len(parts) < 2 or parts[0].lower() != "files" or any(p == ".." or ":" in p for p in parts):
        raise ConfigError(f"files_root must be a folder below 'Files', for example 'Files/dea' (got {value!r})")
    return "/".join(parts)


class Config:
    """Typed, validated view over the p_* notebook parameters."""

    def __init__(self, **values):
        self.__dict__.update(values)

    @classmethod
    def from_namespace(cls, ns):
        values, missing = {}, []
        for kind, names in _PARAM_KINDS.items():
            for name in names:
                key = f"p_{name}"
                if key not in ns:
                    missing.append(key)
                    continue
                raw = ns[key]
                try:
                    if kind == "str":
                        values[name] = "" if raw is None else str(raw).strip()
                    elif kind == "bool":
                        values[name] = _as_bool_param(raw)
                    elif kind == "int":
                        values[name] = int(float(raw))
                    elif kind == "float":
                        values[name] = float(raw)
                    elif kind == "list":
                        values[name] = as_list(raw)
                    else:
                        values[name] = as_dict(raw)
                except (TypeError, ValueError) as exc:
                    raise ConfigError(f"invalid value for {key}: {raw!r}") from exc
        if missing:
            raise ConfigError("missing parameters: " + ", ".join(sorted(missing)))
        cfg = cls(**values)
        cfg.validate()
        return cfg

    def validate(self):
        problems = []

        def need(condition, message):
            if not condition:
                problems.append(message)

        need(self.run_mode in RUN_MODES, f"run_mode must be one of {RUN_MODES}")
        need(self.auth_mode in AUTH_MODES, f"auth_mode must be one of {AUTH_MODES}")
        need(self.preferred_path in PREFERRED_PATHS, f"preferred_path must be one of {PREFERRED_PATHS}")
        need(self.definition_source in DEFINITION_SOURCES, f"definition_source must be one of {DEFINITION_SOURCES}")
        need(self.log_level.upper() in LOG_LEVELS, f"log_level must be one of {LOG_LEVELS}")
        need(1 <= self.scan_batch_size <= 100, "scan_batch_size must be 1..100")
        need(1 <= self.scan_max_concurrent <= 16, "scan_max_concurrent must be 1..16")
        need(1 <= self.max_workers <= 32, "max_workers must be 1..32")
        need(0 < self.rate_limit_scale <= 1, "rate_limit_scale must be in (0, 1]")
        need(self.max_retries >= 0, "max_retries must be >= 0")
        need(self.max_runtime_minutes >= 0, "max_runtime_minutes must be >= 0")
        need(1 <= self.scan_lookback_days <= 30, "scan_lookback_days must be 1..30 (API limit)")
        need(self.run_mode != "resume" or bool(self.resume_run_id), "resume_run_id is required for run_mode=resume")
        need(self.auth_mode != "spn" or bool(self.key_vault_url), "key_vault_url is required for auth_mode=spn")
        need(self.vacuum_retention_hours == 0 or self.vacuum_retention_hours >= MIN_VACUUM_HOURS,
             f"vacuum_retention_hours must be 0 (no VACUUM) or at least {MIN_VACUUM_HOURS}")
        try:
            safe_files_root(self.files_root)
        except ConfigError as exc:
            problems.append(str(exc))
        if problems:
            raise ConfigError("; ".join(problems))

    def public_dict(self):
        """Effective parameters for logging. Only Key Vault secret names are configured, never secrets."""
        return {k: v for k, v in sorted(self.__dict__.items())}

# %% [markdown]
# ## 1.3 Logging, run context, statistics and phase runner
# * `RunContext` carries configuration, clients, storage handles, statistics and the deadline - nothing is global.
# * `run_phase` gives every phase: structured logs, timing, API statistics, `Assessment_RunLog` rows and consistent
#   failure semantics. `GracefulStop` (deadline, API budget, exhausted throttling) yields a **Partial** phase, not a failure.

# %%
class SystemClock:
    def monotonic(self):
        return time.monotonic()

    def sleep(self, seconds):
        time.sleep(max(0.0, seconds))

    def now(self):
        return utcnow()


class LogState:
    run_id = "-"
    phase = "-"


class _ContextFilter(logging.Filter):
    def filter(self, record):
        record.run_id = LogState.run_id
        record.phase = LogState.phase
        return True


class LogCapture(logging.Handler):
    """Keeps the most recent WARNING+ records for the end-of-run report."""

    def __init__(self, capacity=500):
        super().__init__(level=logging.WARNING)
        self.records = deque(maxlen=capacity)

    def emit(self, record):
        self.records.append((record.levelname, record.phase, record.getMessage()))


def setup_logging(level="INFO"):
    logger = logging.getLogger("dea")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(run_id)s | %(phase)-14s | %(message)s", "%H:%M:%S"))
    stream.addFilter(_ContextFilter())
    capture = LogCapture()
    capture.addFilter(_ContextFilter())
    logger.addHandler(stream)
    logger.addHandler(capture)
    return logger, capture


class ApiStats:
    def __init__(self):
        self._lock = threading.Lock()
        self.calls = Counter()
        self.retries = Counter()
        self.throttled = Counter()
        self.failures = Counter()
        self.latency = defaultdict(float)

    def record(self, bucket, status, elapsed, retried=False):
        with self._lock:
            self.calls[bucket] += 1
            self.latency[bucket] += elapsed
            if retried:
                self.retries[bucket] += 1
            if status == 429:
                self.throttled[bucket] += 1
            if status is None or status >= 400:
                self.failures[bucket] += 1

    def totals(self):
        with self._lock:
            return {"calls": sum(self.calls.values()), "retries": sum(self.retries.values()),
                    "throttled": sum(self.throttled.values()), "failures": sum(self.failures.values())}

    def by_bucket(self):
        with self._lock:
            return {b: {"calls": c, "retries": self.retries[b], "throttled": self.throttled[b], "failures": self.failures[b],
                        "avg_ms": round(1000 * self.latency[b] / c, 1) if c else 0.0} for b, c in sorted(self.calls.items())}


class Deadline:
    def __init__(self, minutes, clock):
        self._clock = clock
        self._end = clock.monotonic() + minutes * 60 if minutes else None

    def remaining(self):
        return float("inf") if self._end is None else self._end - self._clock.monotonic()

    def expired(self, reserve_sec=0):
        return self.remaining() <= reserve_sec

    def check(self, reserve_sec=0):
        if self.expired(reserve_sec):
            raise DeadlineExceeded("run deadline reached")


class RunContext:
    def __init__(self, cfg, run_id=None, clock=None, logger=None):
        self.cfg = cfg
        self.run_id = run_id or f"{utcnow():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.clock = clock or SystemClock()
        self.logger = logger or logging.getLogger("dea")
        self.stats = ApiStats()
        self.deadline = Deadline(cfg.max_runtime_minutes, self.clock)
        self.started_at = utcnow()
        self.lake = None
        self.files = None
        self.checkpoints = None
        self.clients = {}
        self.state = {}
        self.phase_rows = []
        self.error_rows = []
        self.phase_status = {}
        self.blocked_phases = set()     # phases skipped because a phase they depend on did not complete (see Step.needs)
        self._seq = 0
        self._error_lock = threading.Lock()
        LogState.run_id = self.run_id

    def continue_sequence(self, last_seq):
        """Number the phases of a resumed run after the ones already logged under the same run id."""
        self._seq = max(self._seq, int(last_seq or 0))

    def record_error(self, phase, entity_type, entity_id, endpoint, exc=None, message=None, attempt=1):
        status = getattr(exc, "status", None)
        row = {
            "run_id": self.run_id, "ts": utcnow(), "phase": phase or LogState.phase, "entity_type": entity_type,
            "entity_id": entity_id, "endpoint": endpoint or getattr(exc, "endpoint", None), "http_status": status,
            "error_code": getattr(exc, "code", None), "message": (message or str(exc))[:2000], "attempt": attempt,
            "retryable": bool(getattr(exc, "retryable", False)),
            "body_snippet": (str(getattr(exc, "body", "") or "")[:1000]) or None,
        }
        with self._error_lock:
            self.error_rows.append(row)
        return row

    def run_phase(self, name, fn, enabled=True, skip_reason="disabled by parameters", critical=False):
        """Run fn(ctx) as a named phase. Returns the metrics dict returned by fn (or {})."""
        self._seq += 1
        started, t0, before = utcnow(), self.clock.monotonic(), self.stats.totals()
        errors_before = len(self.error_rows)
        LogState.phase = name
        row = {"run_id": self.run_id, "seq": self._seq, "phase": name, "started_at": started, "mode": self.cfg.run_mode,
               "params_json": None, "rows_in": None, "rows_out": None, "message": None}
        metrics = {}
        if not enabled:
            row.update(status="Skipped", ended_at=utcnow(), duration_sec=0.0, message=skip_reason,
                       api_calls=0, api_retries=0, api_throttled=0, errors=0)
            self.logger.info("skipped (%s)", skip_reason)
            self.phase_rows.append(row)
            self.phase_status[name] = "Skipped"
            return metrics
        self.logger.info("start")
        status = "Succeeded"
        try:
            metrics = fn(self) or {}
            status = metrics.get("status", "Succeeded")
            row["message"] = metrics.get("message")
        except GracefulStop as stop:
            status = "Partial"
            row["message"] = f"stopped early: {stop}"
            self.logger.warning("stopped early: %s - the next run resumes from the checkpoints", stop)
        except Exception as exc:  # noqa: BLE001 - a phase failure must never lose the run log
            status = "Failed"
            row["message"] = f"{type(exc).__name__}: {exc}"[:2000]
            self.record_error(name, "Phase", name, None, exc=exc, message=row["message"])
            self.logger.error("failed: %s\n%s", row["message"], traceback.format_exc(limit=6))
            if critical:
                self._close_phase(row, status, t0, before, errors_before, metrics)
                raise
        self._close_phase(row, status, t0, before, errors_before, metrics)
        return metrics

    def _close_phase(self, row, status, t0, before, errors_before, metrics):
        after = self.stats.totals()
        row.update(status=status, ended_at=utcnow(), duration_sec=round(self.clock.monotonic() - t0, 2),
                   rows_in=metrics.get("rows_in"), rows_out=metrics.get("rows_out"),
                   api_calls=after["calls"] - before["calls"], api_retries=after["retries"] - before["retries"],
                   api_throttled=after["throttled"] - before["throttled"], errors=len(self.error_rows) - errors_before)
        self.phase_rows.append(row)
        self.phase_status[row["phase"]] = status
        self.logger.info("%s in %.1fs | api calls=%s retries=%s throttled=%s errors=%s", status.lower(), row["duration_sec"],
                         row["api_calls"], row["api_retries"], row["api_throttled"], row["errors"])
        LogState.phase = "-"

    def overall_status(self):
        statuses = set(self.phase_status.values())
        if "Failed" in statuses:
            return "Failed"
        if "Partial" in statuses:
            return "Partial"
        return "Succeeded"


def chunked(items, size):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def gid(value):
    """Normalize ids (GUIDs) for joins: trimmed lowercase, None when empty."""
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def dig(obj, *path, default=None):
    for key in path:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return default
        if obj is None:
            return default
    return obj


def first(*values):
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def percentile(values, q):
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    if len(data) == 1:
        return float(data[0])
    pos = (len(data) - 1) * q
    low, high = math.floor(pos), math.ceil(pos)
    return float(data[low] + (data[high] - data[low]) * (pos - low))


def truncate(text, limit):
    text = "" if text is None else str(text)
    return text if len(text) <= limit else text[: limit - 1] + "…"
