"""Cloudflare invalidation of the imagehoster's avatar/cover redirects, drained from the
replica's outbox (proxy_whitelist_replica.purge_queue).

The syncer enqueues an account in the transaction that commits a changed avatar_url or
cover_url (replica/sync.py); a Purger sends the queued accounts' redirect URLs to
Cloudflare's purge-by-URL API afterwards. It reads and deletes queue rows only, never
replicated tables, and a failed call keeps its rows for a later drain: at-least-once.

Configuration (environment, all three required to enable purging):
  PURGE_CLOUDFLARE_ZONE    the zone id of PURGE_BASE_URL's domain
  PURGE_CLOUDFLARE_TOKEN   an API token with Zone.Cache Purge on that zone
  PURGE_BASE_URL           the imagehoster's public base, e.g. https://images.hive.blog
optional:
  PURGE_URL_TEMPLATES      comma-separated URLs with {base} and {name} placeholders
                           (default DEFAULT_URL_TEMPLATES); one holding "/avatar" is
                           purged on an avatar change, one holding "/cover" on a cover change
  PURGE_TIME_BUDGET_S      seconds one drain may start Cloudflare calls for (default 5)
  PURGE_CLOUDFLARE_API_URL the API base (default https://api.cloudflare.com/client/v4)
"""
import http.client
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol

DEFAULT_URL_TEMPLATES = tuple(
    f"{{base}}{prefix}/u/{{name}}/{path}"
    for prefix in ("", "/webp")
    for path in ("avatar", "avatar/small", "avatar/medium", "avatar/large")
) + ("{base}/u/{name}/cover", "{base}/webp/u/{name}/cover")
DEFAULT_API_URL = "https://api.cloudflare.com/client/v4"
DEFAULT_TIME_BUDGET_S = 5.0
# Cloudflare's limit of files per purge request outside the Enterprise plan.
MAX_URLS_PER_CALL = 30
RETRY_SLEEP_S = 3
MAX_BACKOFF_S = 300
_HTTP_TIMEOUT_S = 10

log = logging.getLogger("proxy_whitelist_replica")


class PurgeError(Exception):
    """A purge call failed; retry_after is the delay Cloudflare asked for (HTTP 429), if any."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class PurgeItem:
    """One purge_queue row: the account and what changed up to its profile's block."""
    account_name: str
    block: int
    avatar_changed: bool
    cover_changed: bool


class PurgeQueue(Protocol):
    def pending_purges(self, limit: int) -> list[PurgeItem]: ...
    def purges_done(self, items: list[PurgeItem]) -> None: ...
    def purges_failed(self, items: list[PurgeItem], error: str) -> None: ...


@dataclass(frozen=True)
class PurgeConfig:
    zone: str
    token: str
    base_url: str
    url_templates: tuple[str, ...] = DEFAULT_URL_TEMPLATES
    time_budget_s: float = DEFAULT_TIME_BUDGET_S
    api_url: str = DEFAULT_API_URL

    def __post_init__(self) -> None:
        if not self.url_templates:
            raise ValueError("PURGE_URL_TEMPLATES names no URL")
        for template in self.url_templates:
            if "{name}" not in template:
                raise ValueError(f"purge URL template {template!r} has no {{name}}")
            if ("/avatar" in template) == ("/cover" in template):
                raise ValueError(f"purge URL template {template!r} must name exactly one of /avatar and /cover")
        if len(self.url_templates) > MAX_URLS_PER_CALL:
            raise ValueError(f"{len(self.url_templates)} purge URL templates, at most {MAX_URLS_PER_CALL}")
        if self.time_budget_s <= 0:
            raise ValueError(f"PURGE_TIME_BUDGET_S must be positive, got {self.time_budget_s}")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "PurgeConfig | None":
        """The configuration, or None (purging off) when a required variable is unset.

        Raises ValueError on an invalid optional variable."""
        required = ("PURGE_CLOUDFLARE_ZONE", "PURGE_CLOUDFLARE_TOKEN", "PURGE_BASE_URL")
        if not all(env.get(name) for name in required):
            return None
        templates = env.get("PURGE_URL_TEMPLATES")
        return cls(
            zone=env["PURGE_CLOUDFLARE_ZONE"],
            token=env["PURGE_CLOUDFLARE_TOKEN"],
            base_url=env["PURGE_BASE_URL"].rstrip("/"),
            url_templates=tuple(t.strip() for t in templates.split(",") if t.strip())
            if templates else DEFAULT_URL_TEMPLATES,
            time_budget_s=float(env.get("PURGE_TIME_BUDGET_S") or DEFAULT_TIME_BUDGET_S),
            api_url=(env.get("PURGE_CLOUDFLARE_API_URL") or DEFAULT_API_URL).rstrip("/"),
        )

    def urls(self, item: PurgeItem) -> list[str]:
        """The redirect URLs of item's account that its change makes stale."""
        return [t.replace("{base}", self.base_url).replace("{name}", item.account_name)
                for t in self.url_templates
                if (item.avatar_changed and "/avatar" in t) or (item.cover_changed and "/cover" in t)]


class CloudflareClient:
    """POST /zones/{zone}/purge_cache with a list of files."""

    def __init__(self, config: PurgeConfig) -> None:
        self._url = f"{config.api_url}/zones/{config.zone}/purge_cache"
        self._token = config.token

    def purge(self, files: list[str]) -> None:
        """Raises PurgeError unless Cloudflare answers success."""
        req = urllib.request.Request(
            self._url, data=json.dumps({"files": files}).encode(), method="POST",
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_S) as resp:
                body = resp.read()
        except urllib.error.HTTPError as err:
            message = f"purge_cache: HTTP {err.code}: {err.read()[:300].decode(errors='replace')}"
            raise PurgeError(message, _retry_after(err.headers.get("Retry-After")) if err.code == 429 else None) from err
        except (urllib.error.URLError, OSError, http.client.HTTPException) as err:
            raise PurgeError(f"purge_cache: {type(err).__name__}: {err}") from err
        try:
            success = json.loads(body).get("success") is True
        except (ValueError, AttributeError):
            success = False
        if not success:
            raise PurgeError(f"purge_cache: unsuccessful answer: {body[:300].decode(errors='replace')}")


def _retry_after(value: str | None) -> float | None:
    try:
        return max(float(value), 0.0) if value is not None else None
    except ValueError:
        return None


class Purger:
    """Drains a purge queue through a client, within a time budget per drain.

    A failed call stops the drain and defers the next one by a backoff of
    RETRY_SLEEP_S doubling to MAX_BACKOFF_S, or by the Retry-After of a 429."""

    def __init__(self, config: PurgeConfig, client: CloudflareClient, queue: PurgeQueue,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._config = config
        self._client = client
        self._queue = queue
        self._clock = clock
        self._backoff: float = RETRY_SLEEP_S
        self._not_before = float("-inf")

    def drain(self) -> None:
        """Purges queued accounts until the queue is empty, the budget is spent or a call fails.

        Raises what the queue raises; a failed purge call is recorded on its rows instead."""
        start = self._clock()
        if start < self._not_before:
            return
        while self._clock() - start < self._config.time_budget_s:
            batch, files = self._next_batch()
            if not batch:
                return
            try:
                self._client.purge(files)
            except PurgeError as err:
                self._queue.purges_failed(batch, str(err))
                delay = err.retry_after if err.retry_after is not None else self._backoff
                self._not_before = self._clock() + delay
                self._backoff = min(self._backoff * 2, MAX_BACKOFF_S)
                log.warning("purge of %d accounts failed: %s; retrying in %s s", len(batch), err, delay)
                return
            self._queue.purges_done(batch)
            self._backoff = RETRY_SLEEP_S
            log.info("purged %d urls of %s", len(files), ", ".join(i.account_name for i in batch))

    def _next_batch(self) -> tuple[list[PurgeItem], list[str]]:
        batch: list[PurgeItem] = []
        files: list[str] = []
        for item in self._queue.pending_purges(MAX_URLS_PER_CALL):
            urls = self._config.urls(item)
            if len(files) + len(urls) > MAX_URLS_PER_CALL:
                break
            batch.append(item)
            files.extend(urls)
        return batch, files
