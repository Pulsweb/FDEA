# %% [markdown]
# # 2. Authentication and API client
# **Identity.** `p_auth_mode`:
# * `spn` - client-credentials flow; tenant id, client id and secret come from **Azure Key Vault** through
#   `notebookutils.credentials.getSecret` (only secret *names* are parameters). Separate tokens are requested for the
#   Power BI (`https://analysis.windows.net/powerbi/api/.default`) and Fabric (`https://api.fabric.microsoft.com/.default`)
#   audiences.
# * `user` - the identity running the notebook (`notebookutils.credentials.getToken("pbi")`), same pattern as the existing
#   example notebooks. Needs the Fabric administrator role for the admin APIs.
# * `auto` - SPN when Key Vault is configured and readable, otherwise the notebook identity.
#
# **Resilience.** One `ApiClient` per API family with: per-bucket sliding-window rate limits (documented quotas x
# `p_rate_limit_scale`), `Retry-After` aware retry with exponential back-off and jitter, token refresh on 401, long-running
# operation polling, and pagination for every response shape used by these APIs. When a wait would exceed
# `p_max_retry_after_sec` the phase stops gracefully and the next run resumes from the checkpoints.

# %%
import base64
import urllib.parse


def jwt_claims(token):
    """Decode (without verifying) the JWT payload for diagnostics. Never log the token itself."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode()))
    except Exception:  # noqa: BLE001
        return {}
    keep = ("aud", "tid", "appid", "azp", "upn", "unique_name", "idtyp", "exp", "roles", "scp")
    return {k: claims[k] for k in keep if k in claims}


class TokenProvider:
    def token(self, audience):
        raise NotImplementedError

    def invalidate(self, audience=None):
        return None

    def describe(self):
        return {"mode": "unknown"}


class SpnTokenProvider(TokenProvider):
    SCOPES = {"pbi": PBI_SCOPE, "fabric": FABRIC_SCOPE, "graph": GRAPH_SCOPE}

    def __init__(self, tenant_id, client_id, client_secret, authority_host, session, clock, margin_sec=300):
        self._tenant, self._client, self._secret = tenant_id, client_id, client_secret
        self._authority = authority_host.rstrip("/")
        self._session, self._clock, self._margin = session, clock, margin_sec
        self._cache = {}
        self._lock = threading.Lock()

    def token(self, audience):
        with self._lock:
            cached = self._cache.get(audience)
            if cached and cached[1] - self._clock.monotonic() > self._margin:
                return cached[0]
            scope = self.SCOPES[audience]
            url = f"{self._authority}/{self._tenant}/oauth2/v2.0/token"
            form = {"grant_type": "client_credentials", "client_id": self._client, "client_secret": self._secret, "scope": scope}
            try:
                resp = self._session.request("POST", url, data=form, timeout=60)
            except requests.RequestException as exc:
                raise AuthError(f"token endpoint unreachable: {type(exc).__name__}", retryable=True) from None
            if resp.status_code != 200:
                detail = ""
                try:
                    detail = (resp.json() or {}).get("error_description", "")[:300]
                except ValueError:
                    pass
                raise AuthError(f"token request for {audience} failed with HTTP {resp.status_code}: {detail}", status=resp.status_code)
            body = resp.json()
            self._cache[audience] = (body["access_token"], self._clock.monotonic() + float(body.get("expires_in", 3600)))
            return body["access_token"]

    def invalidate(self, audience=None):
        with self._lock:
            self._cache.pop(audience, None) if audience else self._cache.clear()

    def describe(self):
        return {"mode": "spn", "client_id": (self._client or "")[:8] + "…", "tenant": (self._tenant or "")[:8] + "…"}


class NotebookTokenProvider(TokenProvider):
    """Identity running the notebook. Fabric accepts the notebook 'pbi' token (same as the Microsoft accelerator)."""

    KEYS = {"pbi": "pbi", "fabric": "pbi"}

    def __init__(self, nbutils):
        self._nbutils = nbutils

    def token(self, audience):
        if audience not in self.KEYS:
            raise AuthError(f"audience {audience} needs a service principal (set p_auth_mode = spn)")
        return self._nbutils.credentials.getToken(self.KEYS[audience])

    def describe(self):
        return {"mode": "user"}


def resolve_identity(cfg, nbutils, session, clock, logger):
    """Pick the token provider. SPN secrets are read from Key Vault and never logged."""
    want_spn = cfg.auth_mode == "spn" or (cfg.auth_mode == "auto" and bool(cfg.key_vault_url))
    if want_spn:
        try:
            secrets = [nbutils.credentials.getSecret(cfg.key_vault_url, name) for name in
                       (cfg.kv_secret_tenant_id, cfg.kv_secret_client_id, cfg.kv_secret_client_secret)]
            provider = SpnTokenProvider(*secrets, cfg.authority_host, session, clock)
            provider.token("pbi")
            logger.info("authenticated as service principal %s", provider.describe()["client_id"])
            return provider
        except Exception as exc:  # noqa: BLE001
            if cfg.auth_mode == "spn":
                raise AuthError(f"service principal authentication failed: {type(exc).__name__}: {str(exc)[:200]}") from None
            logger.warning("service principal unavailable (%s) - falling back to the notebook identity", type(exc).__name__)
    if nbutils is None:
        raise AuthError("no notebook identity available outside Fabric; configure the service principal")
    provider = NotebookTokenProvider(nbutils)
    claims = jwt_claims(provider.token("pbi"))
    logger.info("authenticated as notebook identity %s", claims.get("upn") or claims.get("unique_name") or claims.get("appid") or "?")
    return provider

# %%
# Documented quotas as (calls, window_seconds). A call may be subject to several buckets; the most restrictive binds.
DEFAULT_RATE_LIMITS = {
    "pbi_scan_modified": [(30, 3600)],
    "pbi_scan_getinfo": [(500, 3600)],
    "pbi_scan_status": [(10000, 3600)],
    "pbi_scan_result": [(500, 3600)],
    "pbi_admin_groups": [(15, 60), (50, 3600)],
    "pbi_admin_capacities": [(200, 3600)],
    "pbi_admin_pipelines": [(200, 3600)],
    "pbi_admin_dataflows": [(200, 3600)],
    "pbi_admin_dataflow_export": [(200, 3600)],
    "pbi_admin_dataflow_datasources": [(200, 3600)],
    "pbi_admin_dataflow_upstream": [(200, 3600)],
    "pbi_admin_dataflow_users": [(200, 3600)],
    "pbi_admin_dataset_links": [(200, 3600)],
    "pbi_dataflow": [(3000, 3600)],
    "fabric_admin_items": [(200, 3600)],
    "fabric_admin_workspaces": [(200, 3600)],
    "fabric_admin_domains": [(25, 60)],
    "fabric_admin_tags": [(25, 60)],
    "fabric_platform": [(500, 60)],
    "fabric_jobs": [(200, 60)],
    "fabric_lro": [(500, 60)],
    "graph": [(600, 60)],
}


class RateLimiter:
    """Thread-safe sliding-window limiter with server-driven penalties (Retry-After)."""

    def __init__(self, rules, clock, scale=1.0, max_wait=None):
        self._rules = {b: [(max(1, int(n * scale)), w) for n, w in r] for b, r in rules.items()}
        self._hits = defaultdict(deque)
        self._blocked = {}
        self._clock = clock
        self._lock = threading.Lock()
        self.max_wait = max_wait

    def _wait_needed(self, bucket, now):
        wait = max(0.0, self._blocked.get(bucket, 0.0) - now)
        for calls, window in self._rules.get(bucket, []):
            hits = self._hits[(bucket, window)]
            while hits and hits[0] <= now - window:
                hits.popleft()
            if len(hits) >= calls:
                wait = max(wait, hits[0] + window - now)
        return wait

    def acquire(self, buckets):
        buckets = (buckets,) if isinstance(buckets, str) else tuple(buckets)
        while True:
            with self._lock:
                now = self._clock.monotonic()
                wait = max(self._wait_needed(b, now) for b in buckets)
                if wait <= 0:
                    for b in buckets:
                        for _, window in self._rules.get(b, []):
                            self._hits[(b, window)].append(now)
                    return
            if self.max_wait is not None and wait > self.max_wait:
                raise ThrottleExhausted(f"rate limit on {buckets[0]} requires waiting {int(wait)}s (> {int(self.max_wait)}s allowed)", status=429)
            self._clock.sleep(min(wait, 60.0) + 0.05)

    def penalize(self, buckets, seconds):
        buckets = (buckets,) if isinstance(buckets, str) else tuple(buckets)
        with self._lock:
            until = self._clock.monotonic() + seconds
            for b in buckets:
                self._blocked[b] = max(self._blocked.get(b, 0.0), until)


def build_rate_limits(cfg):
    rules = {b: list(r) for b, r in DEFAULT_RATE_LIMITS.items()}
    for bucket, windows in cfg.rate_limit_overrides.items():
        rules[bucket] = [(int(n), int(w)) for n, w in windows]
    return rules


def parse_retry_after(resp, default=None):
    value = resp.headers.get("Retry-After") if resp.headers else None
    if value:
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                from email.utils import parsedate_to_datetime
                when = parsedate_to_datetime(value)
                return max(0.0, (when - dt.datetime.now(when.tzinfo)).total_seconds())
            except (TypeError, ValueError):
                pass
    match = re.search(r"[Rr]etry (?:in|after) (\d+)", getattr(resp, "text", "") or "")
    return float(match.group(1)) if match else default


def error_details(resp):
    """Extract (code, message) from Fabric ({errorCode, message}) and Power BI ({error:{code,message}}) bodies."""
    try:
        body = resp.json()
    except ValueError:
        return None, (resp.text or "")[:300]
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            return err.get("code"), err.get("message")
        return body.get("errorCode") or body.get("code"), body.get("message") or (err if isinstance(err, str) else None)
    return None, str(body)[:300]


class ThreadSessions:
    """One requests.Session per thread (a Session is not documented as thread-safe) behind the single call the notebook uses.

    The factory looks `requests.Session` up when a thread first needs it, so replacing it (tests, proxies) keeps working."""

    def __init__(self, factory=None):
        self._factory = factory or (lambda: requests.Session())
        self._local = threading.local()

    def request(self, *args, **kwargs):
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._local.session = self._factory()
        return session.request(*args, **kwargs)


class ApiClient:
    RETRYABLE = (408, 500, 502, 503, 504)

    def __init__(self, name, base_url, audience, tokens, limiter, stats, clock, cfg, session=None, logger=None, rng=None):
        self.name = name
        self.base = base_url.rstrip("/")
        self.audience = audience
        self.tokens = tokens
        self.limiter = limiter
        self.stats = stats
        self.clock = clock
        self.cfg = cfg
        self.session = session or ThreadSessions()
        self.logger = logger or logging.getLogger("dea")
        self.rng = rng or random.Random()
        self.user_agent = f"DataflowEstateAssessment/{NOTEBOOK_VERSION}"

    def url(self, path):
        """Relative paths hang off the base URL. Absolute URLs (continuation links, operation locations) must stay on the base host:
        the bearer token is never sent anywhere else."""
        if not path.startswith(("http://", "https://")):
            return f"{self.base}/{path.lstrip('/')}"
        target, home = urllib.parse.urlparse(path), urllib.parse.urlparse(self.base)
        if (target.hostname or "").lower() != (home.hostname or "").lower() or (target.scheme != home.scheme and target.scheme != "https"):
            raise ApiError(f"refusing to send credentials to another host ({self._label(path) or target.hostname})", endpoint=self._label(path))
        return path

    def _backoff(self, attempt):
        delay = min(self.cfg.retry_max_delay_sec, self.cfg.retry_base_delay_sec * (2 ** (attempt - 1)))
        return delay + self.rng.uniform(0, delay * 0.25)

    def request(self, method, path, params=None, json_body=None, headers=None, buckets=("default",), ok=(200,), allow=(), timeout=120):
        """Send a request with throttling, retries and error classification. Returns the response for ok/allow statuses."""
        url = self.url(path)
        attempt, refreshed = 0, False
        while True:
            attempt += 1
            self.limiter.acquire(buckets)
            hdrs = {"Authorization": f"Bearer {self.tokens.token(self.audience)}", "User-Agent": self.user_agent,
                    "Accept": "application/json", **(headers or {})}
            started = self.clock.monotonic()
            try:
                resp = self.session.request(method, url, params=params, json=json_body, headers=hdrs, timeout=timeout)
            except requests.RequestException as exc:
                self.stats.record(buckets[0], None, self.clock.monotonic() - started, retried=attempt > 1)
                if attempt <= self.cfg.max_retries:
                    self.clock.sleep(self._backoff(attempt))
                    continue
                raise ApiError(f"{method} {self._label(path)} failed: {type(exc).__name__}", retryable=True, endpoint=self._label(path)) from None
            status = resp.status_code
            self.stats.record(buckets[0], status, self.clock.monotonic() - started, retried=attempt > 1)
            if status in ok or status in allow:
                return resp
            if status == 401 and not refreshed:
                refreshed = True
                self.tokens.invalidate(self.audience)
                continue
            if status == 429:
                wait = parse_retry_after(resp, default=self._backoff(attempt))
                self.limiter.penalize(buckets, wait)
                if wait > self.cfg.max_retry_after_sec or attempt > self.cfg.max_retries:
                    raise ThrottleExhausted(f"{self._label(path)} throttled (Retry-After {int(wait)}s)", status=429, endpoint=self._label(path), retryable=True)
                self.logger.warning("429 on %s - waiting %ds (attempt %d/%d)", buckets[0], int(wait), attempt, self.cfg.max_retries)
                self.clock.sleep(wait + self.rng.uniform(0, 1.0))
                continue
            if status in self.RETRYABLE and attempt <= self.cfg.max_retries:
                self.clock.sleep(self._backoff(attempt))
                continue
            raise self._classify(resp, method, path)

    def _label(self, path):
        text = re.sub(r"https?://[^/]+", "", path)
        text = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "{id}", text)
        return text.split("?")[0]

    def _classify(self, resp, method, path):
        code, message = error_details(resp)
        status = resp.status_code
        text = f"{method} {self._label(path)} -> HTTP {status} {code or ''} {message or ''}".strip()
        kwargs = dict(status=status, code=code, url=self._label(path), body=(resp.text or "")[:1000], endpoint=self._label(path),
                      retryable=status in self.RETRYABLE)
        if status == 401:
            return AuthError(text, **kwargs)
        if status == 403:
            return ForbiddenError(text, **kwargs)
        if status == 404:
            return NotFoundError(text, **kwargs)
        return ApiError(text, **kwargs)

    def get_json(self, path, params=None, buckets=("default",), **kw):
        resp = self.request("GET", path, params=params, buckets=buckets, **kw)
        return resp.json() if resp.content else {}

    def post_json(self, path, body=None, params=None, buckets=("default",), ok=(200,), **kw):
        resp = self.request("POST", path, params=params, json_body=body if body is not None else {}, buckets=buckets, ok=ok, **kw)
        return resp.json() if resp.content else {}

    def paginate(self, path, params=None, items_key=None, style="fabric", buckets=("default",), page_size=5000, max_pages=100000, **kw):
        """Yield items across pages.

        style='fabric' follows continuationUri / continuationToken; style='skip' uses $top/$skip (Power BI admin APIs).
        The items key is auto-detected (value, itemEntities, workspaces, ...) unless given.
        """
        params = dict(params or {})
        url, pages, seen = path, 0, set()
        previous_first = None
        if style == "skip":
            params.setdefault("$top", page_size)
            params.setdefault("$skip", 0)
        while pages < max_pages:
            pages += 1
            body = self.get_json(url, params=dict(params), buckets=buckets, **kw)
            items = self._items(body, items_key)
            if style == "skip":
                first_key = json.dumps(items[0], sort_keys=True, default=str) if items else None
                if not items or first_key == previous_first:
                    return
                previous_first = first_key
            for item in items:
                yield item
            if style == "skip":
                params["$skip"] = int(params["$skip"]) + len(items)
                continue
            next_uri, token = body.get("continuationUri"), body.get("continuationToken")
            cursor = next_uri or token
            if not cursor or cursor in seen:
                return
            seen.add(cursor)
            if next_uri:
                url, params = next_uri, {}
            else:
                params["continuationToken"] = token

    @staticmethod
    def _items(body, items_key):
        if isinstance(body, list):
            return body
        if items_key:
            return body.get(items_key) or []
        for key in ("value", "itemEntities", "workspaces", "domains", "tags", "capacities", "connections", "gateways",
                    "activityEventEntities", "accessDetails", "items"):
            if isinstance(body.get(key), list):
                return body[key]
        lists = [v for v in body.values() if isinstance(v, list) and (not v or isinstance(v[0], dict))]
        return lists[0] if len(lists) == 1 else []

    def lro(self, resp, timeout_sec=600, require_result=True):
        """Complete a Fabric long-running operation (202 + Location / x-ms-operation-id) and return its result body.

        `Retry-After` is honored (the next poll is never earlier than the service asked). `timeout_sec` bounds the time spent *waiting*: no sleep goes past it and
        a zero timeout never polls; the poll and result requests keep their own HTTP timeout and retry policy, so the call can end one request after the deadline.
        A succeeded operation whose result cannot be read is an error, unless the caller expects an operation without result."""
        if resp.status_code != 202:
            return resp.json() if resp.content else {}
        op_id = resp.headers.get("x-ms-operation-id")
        status_url = resp.headers.get("Location") or f"{self.base}/operations/{op_id}"
        delay = parse_retry_after(resp, default=5.0)
        deadline = self.clock.monotonic() + timeout_sec
        while True:
            remaining = deadline - self.clock.monotonic()
            if remaining <= 0:
                raise ApiError("long-running operation timed out", endpoint="operations", retryable=True)
            self.clock.sleep(min(max(delay, 1.0), remaining))
            poll = self.request("GET", status_url, buckets=("fabric_lro",))
            body = poll.json() if poll.content else {}
            state = body.get("status")
            if state == "Succeeded":
                result_url = poll.headers.get("Location") or status_url.rstrip("/") + "/result"
                result = self.request("GET", result_url, buckets=("fabric_lro",), allow=(404, 204))
                if result.status_code == 200 and result.content:
                    return result.json()
                if require_result:
                    label = f"operation {op_id}" if op_id else "operation"
                    raise ApiError(f"{label} succeeded but its result is not available (HTTP {result.status_code})", status=result.status_code,
                                   endpoint="operations", retryable=True)
                return {}
            if state in ("Failed", "Cancelled"):
                err = body.get("error") or {}
                raise ApiError(f"operation {state}: {err.get('errorCode', '')} {err.get('message', '')}".strip(), code=err.get("errorCode"), endpoint="operations")
            delay = parse_retry_after(poll, default=5.0)

# %%
def build_clients(ctx, tokens, session=None):
    cfg = ctx.cfg
    limiter = RateLimiter(build_rate_limits(cfg), ctx.clock, scale=cfg.rate_limit_scale, max_wait=cfg.max_retry_after_sec)
    http = session or ThreadSessions()
    make = lambda name, base, audience: ApiClient(name, base, audience, tokens, limiter, ctx.stats, ctx.clock, cfg, http, ctx.logger)  # noqa: E731
    ctx.clients = {"pbi": make("pbi", cfg.pbi_api_base, "pbi"), "fabric": make("fabric", cfg.fabric_api_base, "fabric"),
                   "graph": make("graph", cfg.graph_api_base, "graph")}
    ctx.limiter = limiter
    return ctx.clients


def preflight(ctx):
    """Probe the API families and record what is accessible; later phases degrade gracefully on denied families."""
    probes = {
        "pbi_admin": ("pbi", "admin/capacities", ("pbi_admin_capacities",)),
        "fabric_admin": ("fabric", "admin/tags", ("fabric_admin_tags", "fabric_platform")),
        "fabric_core": ("fabric", "capacities", ("fabric_platform",)),
    }
    access = {}
    for key, (client, path, buckets) in probes.items():
        try:
            ctx.clients[client].request("GET", path, buckets=buckets)
            access[key] = "OK"
        except ThrottleExhausted:
            raise
        except ApiError as exc:
            access[key] = f"Denied ({exc.status})" if exc.status in (401, 403) else f"Error ({exc.status})"
            ctx.record_error("preflight", "Api", key, exc.endpoint, exc=exc)
    ctx.state["access"] = access
    hints = {
        "pbi_admin": "enable 'Service principals can access read-only admin APIs' for a security group containing the SPN; the app must have no admin-consent Power BI permissions",
        "fabric_admin": "enable 'Service principals can access read-only admin APIs' (Fabric admin APIs also need the Fabric administrator role for users)",
        "fabric_core": "enable 'Service principals can call Fabric public APIs'",
    }
    for key, state in access.items():
        (ctx.logger.info if state == "OK" else ctx.logger.warning)("access %-12s %s%s", key, state, "" if state == "OK" else f"  -> {hints[key]}")
    return {"rows_out": len(access), "message": ", ".join(f"{k}={v}" for k, v in access.items())}

# %% [markdown]
# ### Run - sections 1 and 2: configuration and authentication
# Validates the parameters, wires logging, storage and identity, then probes the API families (`preflight`, `access_check`).

# %% tags=["run"]
ctx = start_run(globals())
run_steps(ctx, sections={2})
