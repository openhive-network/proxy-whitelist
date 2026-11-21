#!/usr/bin/env python3
"""Follow a proxy-whitelist primary's replication streams into a replica database.

    sync.py [--once] [--reset] [--rebuild]

Configuration (environment):
  PROXY_WHITELIST_SOURCE_URL       the primary's API base, e.g. http://10.10.10.8/proxy-whitelist-api
  POSTGRES_URL                     the replica database (installed by replica/install.sh), as
                                   its owner or a superuser
  POLL_INTERVAL_S                  seconds between cycles (default 5)
  PAGE_LIMIT                       page_size of every stream request (default and maximum 100000)
  PROXY_WHITELIST_REPLICA_RESET    1 to act as --reset
  PROXY_WHITELIST_REPLICA_REBUILD  1 to act as --rebuild once: the replica records that it
                                   rebuilt for it and ignores it on later starts until it has
                                   been seen unset
  PURGE_CLOUDFLARE_ZONE, PURGE_CLOUDFLARE_TOKEN, PURGE_BASE_URL, PURGE_URL_TEMPLATES,
  PURGE_TIME_BUDGET_S, PURGE_CLOUDFLARE_API_URL
                                   Cloudflare invalidation of the avatar/cover redirects
                                   (replica/purge.py); off unless the first three are set

One cycle reads the source's /sync-status, checks the source identity
(sync_uuid, stream_schema_version) against the one stored on first contact,
pages every stream (contract v2, endpoints/stream.sql) from its cursor up to
the source's last_block_num, replaces the blacklists and records the source's
block as fully replicated. Each page is applied together with its cursor in
one transaction, so a failure never loses or skips a page: the next cycle
resumes from the last applied one.

A changed source identity sets sync_error and writes nothing more while the
replica keeps serving its data: the syncer checks the source again every
POLL_INTERVAL_S and resumes once the identity matches (--once: exit 2). With
--reset the replicated tables and cursors are truncated instead and the
bootstrap starts again; --rebuild does that before the first cycle whether or
not the identity changed. A page the source path gives up on (HTTP 502/504, a
timeout) is retried at once with half the stream's page size, down to
MIN_PAGE_SIZE. Other failures set sync_error and are retried with a backoff of
3 s doubling to 60 s (--once: exit 1).

With purging on, a profile page whose rows change an account's avatar_url or
cover_url enqueues the account into proxy_whitelist_replica.purge_queue in the
page's transaction, unless the cycle started before bootstrap_complete (a
bootstrap, --reset or --rebuild purges nothing). The queue is drained after
every page that enqueued and at the end of every cycle; a drain failure is
logged and left to the purger's own backoff, never to sync_error.
"""
import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TypeVar

import psycopg
from psycopg import sql

from purge import CloudflareClient, PurgeConfig, PurgeItem, Purger

STREAM_SCHEMA_VERSION = 2
# The source's stream_page_cap(): a larger page_size would be served capped,
# and a capped page would read as a short (range-completing) one.
MAX_PAGE_SIZE = 100000
# The smallest page a slow source path is retried with (or PAGE_LIMIT when lower).
MIN_PAGE_SIZE = 1000
RETRY_SLEEP_S = 3
MAX_BACKOFF_S = 60
HTTP_TIMEOUT_S = 300
# A page body below this may arrive uncompressed from a source that compresses
# larger ones (caddy's encode minimum_length), so it decides no compression verdict.
_COMPRESSIBLE_PAGE_BYTES = 1024

log = logging.getLogger("proxy_whitelist_replica")
_T = TypeVar("_T")


class SyncError(Exception):
    """A cycle could not complete; the cursors stay at the last applied page."""


class IdentityMismatch(SyncError):
    """The source is not the one the replica bootstrapped from (or speaks another contract)."""


class SourceTimeout(SyncError):
    """The source, or a proxy in front of it, gave up on a request (HTTP 502/504 or a timeout)."""


@dataclass(frozen=True)
class Stream:
    """One /stream/* endpoint: its row fields and its (block, natural key) cursor."""
    name: str
    path: str
    block_field: str
    key_fields: tuple[str, ...]
    after_params: tuple[str, ...]
    int_fields: tuple[str, ...]


ACCOUNTS = Stream("accounts", "/stream/accounts", "first_block", ("name",), ("after_name",),
                  ("first_block", "hive_id"))
URLS = Stream("urls", "/stream/urls", "first_block", ("md5",), ("after_md5",),
              ("first_block",))
URL_AUTHORS = Stream("url_authors", "/stream/url-authors", "first_block", ("url_md5", "account_name"),
                     ("after_md5", "after_account"), ("first_block",))
PROFILES = Stream("profiles", "/stream/profiles", "updated_block", ("account_name",), ("after_account",),
                  ("updated_block",))
# Applied in this order within a cycle: accounts and urls before the rows that
# reference them.
STREAMS = (ACCOUNTS, URLS, URL_AUTHORS, PROFILES)


@dataclass(frozen=True)
class Cursor:
    """Every row up to block `block` is applied; when `key` is set, only up to that key within it."""
    block: int = -1
    key: tuple[str, ...] | None = None

    def __str__(self) -> str:
        return f"{self.block}" if self.key is None else f"{self.block}/{'/'.join(self.key)}"


@dataclass(frozen=True)
class SourceStatus:
    block_num: int
    block_time: datetime | None
    sync_uuid: str
    stream_schema_version: int
    stream_sentinel_block: int | None


@dataclass(frozen=True)
class Page:
    rows: list[dict[str, Any]]
    to_block: int
    sync_uuid: str | None


@dataclass(frozen=True)
class State:
    sync_uuid: str | None
    stream_schema_version: int | None
    cursors: dict[str, Cursor]
    bootstrap_complete: bool
    rebuild_env_honoured: bool = False


class Source(Protocol):
    def status(self) -> SourceStatus: ...
    def page(self, stream: Stream, from_block: int, to_block: int,
             after: dict[str, str], page_size: int) -> Page: ...
    def blacklists(self) -> tuple[list[str], list[str]]: ...


class Store(Protocol):
    def load(self) -> State: ...
    def adopt_source(self, source_url: str, status: SourceStatus) -> None: ...
    def reset(self, source_url: str, status: SourceStatus, rebuild_env_honoured: bool) -> None: ...
    def set_rebuild_env_honoured(self, honoured: bool) -> None: ...
    def apply_page(self, stream: Stream, rows: list[dict[str, Any]], cursor: Cursor,
                   enqueue_purges: bool = False) -> int: ...
    def replace_blacklists(self, urls: list[str], accounts: list[str]) -> None: ...
    def finish_cycle(self, source_url: str, status: SourceStatus) -> None: ...
    def set_error(self, message: str) -> None: ...
    def set_purge_enabled(self, enabled: bool) -> None: ...
    def queued_purges(self) -> int: ...


def _parse_block_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


class HttpSource:
    """The primary's API through its URL rewriter (the paths of rewrite_rules.conf)."""

    def __init__(self, base_url: str, timeout: float = HTTP_TIMEOUT_S) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._compression_logged = False

    def _get(self, path: str) -> tuple[bytes, dict[str, str]]:
        req = urllib.request.Request(self._base + path,
                                     headers={"Accept": "application/json", "Accept-Encoding": "gzip"})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body, headers = resp.read(), {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as err:
            body = err.read()[:500].decode(errors="replace")
            message = f"GET {path}: HTTP {err.code}: {body}"
            if err.code in (502, 504):
                raise SourceTimeout(message) from err
            raise SyncError(message) from err
        except TimeoutError as err:
            raise SourceTimeout(f"GET {path}: no answer within {self._timeout} s") from err
        except urllib.error.URLError as err:
            if isinstance(err.reason, TimeoutError):
                raise SourceTimeout(f"GET {path}: no answer within {self._timeout} s") from err
            raise SyncError(f"GET {path}: {err}") from err
        except OSError as err:
            raise SyncError(f"GET {path}: {err}") from err
        encoding = headers.get("content-encoding", "identity").strip().lower()
        if encoding == "gzip":
            try:
                body = gzip.decompress(body)
            except (OSError, EOFError, zlib.error) as err:
                raise SyncError(f"GET {path}: undecodable gzip body: {type(err).__name__}: {err}") from err
        elif encoding != "identity":
            raise SyncError(f"GET {path}: unsupported Content-Encoding {encoding!r}")
        return body, headers

    def _get_json(self, path: str) -> Any:
        body, _ = self._get(path)
        try:
            return json.loads(body)
        except ValueError as err:
            raise SyncError(f"GET {path}: not JSON: {err}") from err

    def status(self) -> SourceStatus:
        data = self._get_json("/sync-status")
        if isinstance(data, list) and len(data) == 1:
            data = data[0]
        try:
            return SourceStatus(
                block_num=int(data["last_block_num"]),
                block_time=_parse_block_time(data["last_block_time"]),
                sync_uuid=str(data["sync_uuid"]),
                stream_schema_version=int(data["stream_schema_version"]),
                stream_sentinel_block=data.get("stream_sentinel_block"),
            )
        except (KeyError, TypeError, ValueError) as err:
            raise SyncError(f"GET /sync-status: unexpected answer {data!r}: {err}") from err

    def page(self, stream: Stream, from_block: int, to_block: int,
             after: dict[str, str], page_size: int) -> Page:
        query = urllib.parse.urlencode(
            {"from_block": from_block, "to_block": to_block, **after, "page_size": page_size})
        path = f"{stream.path}?{query}"
        body, headers = self._get(path)
        if not self._compression_logged:
            self._log_compression(body, headers)
        try:
            rows: list[dict[str, Any]] = json.loads(body)
            for row in rows:
                for field in stream.int_fields:
                    row[field] = int(row[field])
            to_block = int(headers["stream-to-block"])
        except (ValueError, KeyError, TypeError) as err:
            raise SyncError(f"GET {path}: undecodable page: {type(err).__name__}: {err}") from err
        return Page(rows, to_block, headers.get("stream-sync-uuid"))

    def _log_compression(self, body: bytes, headers: dict[str, str]) -> None:
        if headers.get("content-encoding", "").strip().lower() == "gzip":
            log.info("the source compresses stream pages (Content-Encoding: gzip)")
        elif len(body) >= _COMPRESSIBLE_PAGE_BYTES:
            log.info("the source does not compress stream pages; a page of %d bytes arrived uncompressed", len(body))
        else:
            return
        self._compression_logged = True

    def blacklists(self) -> tuple[list[str], list[str]]:
        data = self._get_json("/stream/blacklists")
        try:
            return [str(u) for u in data["urls"]], [str(a) for a in data["accounts"]]
        except (KeyError, TypeError) as err:
            raise SyncError(f"GET /stream/blacklists: unexpected answer: {err}") from err


class Syncer:
    """One sync cycle at a time, from a source into a store.

    rebuild truncates the replica before the first cycle; rebuild_from_env does
    the same unless the store records that it already did so for the environment
    variable, and when the variable is unset the record is cleared. With a purger,
    profile changes applied after the bootstrap are enqueued and drained by it."""

    def __init__(self, source: Source, store: Store, source_url: str, page_size: int, reset: bool,
                 rebuild: bool = False, rebuild_from_env: bool = False, purger: Purger | None = None) -> None:
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"page size must be within [1, {MAX_PAGE_SIZE}], got {page_size}")
        self.source = source
        self.store = store
        self.source_url = source_url
        self.page_size = page_size
        self.reset = reset
        self._rebuild = rebuild
        self._rebuild_from_env = rebuild_from_env
        self._rebuild_checked = False
        self._page_sizes = {stream.name: page_size for stream in STREAMS}
        self._purger = purger
        self._purge_enabled_recorded = False

    def cycle(self) -> SourceStatus:
        """Replicates the source up to its current last_block_num; returns the status it reached."""
        try:
            status = self.source.status()
            state = self._checked_state(status)
            enqueue = self._purger is not None and state.bootstrap_complete
            for stream in STREAMS:
                self._follow(stream, state.cursors[stream.name], status, enqueue)
            urls, accounts = self.source.blacklists()
            self.store.replace_blacklists(urls, accounts)
            self.store.finish_cycle(self.source_url, status)
            if not state.bootstrap_complete:
                log.info("bootstrap complete at block %s", status.block_num)
            return status
        finally:
            self._drain_purges()

    def _drain_purges(self) -> None:
        if self._purger is None:
            return
        try:
            self._purger.drain()
        except SyncError as err:
            log.warning("purge queue: %s; draining again next cycle", err)

    def _checked_state(self, status: SourceStatus) -> State:
        if status.stream_schema_version != STREAM_SCHEMA_VERSION:
            raise IdentityMismatch(
                f"the source serves stream_schema_version {status.stream_schema_version}, "
                f"this syncer reads {STREAM_SCHEMA_VERSION}")
        state = self.store.load()
        if not self._rebuild_checked:
            state = self._rebuilt(state, status)
        if not self._purge_enabled_recorded:
            self._record_purge_enabled()
        if state.sync_uuid is None:
            log.info("first contact with %s: sync_uuid %s", self.source_url, status.sync_uuid)
            self.store.adopt_source(self.source_url, status)
            return self.store.load()
        if (state.sync_uuid, state.stream_schema_version) != (status.sync_uuid, status.stream_schema_version):
            mismatch = (f"source identity changed: sync_uuid {state.sync_uuid} -> {status.sync_uuid}, "
                        f"stream_schema_version {state.stream_schema_version} -> {status.stream_schema_version}")
            if not self.reset:
                raise IdentityMismatch(mismatch)
            log.warning("%s; resetting the replica and bootstrapping again", mismatch)
            self.store.reset(self.source_url, status, state.rebuild_env_honoured)
            return self.store.load()
        return state

    def _record_purge_enabled(self) -> None:
        self.store.set_purge_enabled(self._purger is not None)
        self._purge_enabled_recorded = True
        if self._purger is None:
            queued = self.store.queued_purges()
            if queued:
                log.warning("%d accounts are queued for purging but purging is off; they stay queued", queued)

    def _rebuilt(self, state: State, status: SourceStatus) -> State:
        """Truncates the replica when a rebuild is requested and not yet honoured; returns the state to sync from."""
        env_pending = self._rebuild_from_env and not state.rebuild_env_honoured
        if self._rebuild or env_pending:
            via = "--rebuild" if self._rebuild else "PROXY_WHITELIST_REPLICA_REBUILD=1"
            log.warning("%s: truncating the replica and bootstrapping again from %s", via, self.source_url)
            self.store.reset(self.source_url, status, self._rebuild_from_env)
            state = self.store.load()
        elif state.rebuild_env_honoured and not self._rebuild_from_env:
            self.store.set_rebuild_env_honoured(False)
        elif self._rebuild_from_env:
            log.info("PROXY_WHITELIST_REPLICA_REBUILD=1 was honoured by an earlier start; "
                     "unset it to allow another rebuild")
        self._rebuild_checked = True
        return state

    def _follow(self, stream: Stream, cursor: Cursor, status: SourceStatus, enqueue_purges: bool) -> None:
        target = status.block_num
        if cursor.block > target:
            raise SyncError(f"{stream.name}: the source's last_block_num {target} is below the replicated block {cursor}")
        while True:
            if cursor.key is None:
                from_block, after = cursor.block + 1, {}
            else:
                from_block, after = cursor.block, dict(zip(stream.after_params, cursor.key))
            if from_block > target:
                return
            try:
                cursor = self._apply_next(stream, from_block, after, target, status.sync_uuid, enqueue_purges)
            except IdentityMismatch:
                raise
            except SourceTimeout as err:
                if not self._smaller_page(stream, err):
                    raise SyncError(f"{stream.name} at cursor {cursor}: {err}") from err
            except SyncError as err:
                raise SyncError(f"{stream.name} at cursor {cursor}: {err}") from err

    def _smaller_page(self, stream: Stream, err: SourceTimeout) -> bool:
        """Halves the stream's page size after a page the source path gave up on; False at the floor."""
        size = self._page_sizes[stream.name]
        smaller = max(size // 2, min(MIN_PAGE_SIZE, self.page_size))
        if smaller == size:
            return False
        log.warning("%s: %s; retrying with page_size %d instead of %d", stream.name, err, smaller, size)
        self._page_sizes[stream.name] = smaller
        return True

    def _apply_next(self, stream: Stream, from_block: int, after: dict[str, str],
                    target: int, sync_uuid: str, enqueue_purges: bool) -> Cursor:
        page_size = self._page_sizes[stream.name]
        page = self.source.page(stream, from_block, target, after, page_size)
        if page.sync_uuid != sync_uuid:
            raise IdentityMismatch(f"{stream.name}: page from Stream-Sync-Uuid {page.sync_uuid}, expected {sync_uuid}")
        rows = page.rows
        if len(rows) > page_size:
            raise SyncError(f"page of {len(rows)} rows for page_size {page_size}")
        if len(rows) == page_size:
            last = rows[-1]
            if page.to_block != last[stream.block_field]:
                raise SyncError(f"full page ends at block {last[stream.block_field]}, Stream-To-Block {page.to_block}")
            cursor = Cursor(page.to_block, tuple(str(last[f]) for f in stream.key_fields))
        else:
            if page.to_block != target:
                raise SyncError(f"short page with Stream-To-Block {page.to_block}, requested up to {target}")
            cursor = Cursor(page.to_block)
        queued = self.store.apply_page(stream, rows, cursor, enqueue_purges)
        log.debug("%s: %d rows, cursor %s", stream.name, len(rows), cursor)
        if queued:
            log.info("%s: %d accounts queued for purging", stream.name, queued)
            self._drain_purges()
        if page_size != self.page_size:
            log.info("%s: page_size %d passed; back to %d", stream.name, page_size, self.page_size)
            self._page_sizes[stream.name] = self.page_size
        return cursor


class PgStore:
    """The replica database installed by replica/install.sh."""

    _REPLICATED_TABLES = (
        "proxy_whitelist_app.blacklisted_urls", "proxy_whitelist_app.blacklisted_accounts",
        "proxy_whitelist_app.account_profiles", "proxy_whitelist_app.url_authors",
        "proxy_whitelist_app.urls", "proxy_whitelist_app.accounts",
        "proxy_whitelist_replica.pending_url_authors", "proxy_whitelist_replica.purge_queue",
    )

    def __init__(self, postgres_url: str) -> None:
        self._url = postgres_url
        self._conn: psycopg.Connection | None = None
        self._apply: dict[str, Callable[[psycopg.Cursor, list[dict[str, Any]], bool], int]] = {
            ACCOUNTS.name: self._apply_accounts,
            URLS.name: self._apply_urls,
            URL_AUTHORS.name: self._apply_url_authors,
            PROFILES.name: self._apply_profiles,
        }

    def _transaction(self, work: Callable[[psycopg.Cursor], _T]) -> _T:
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg.connect(self._url)
            with self._conn.transaction(), self._conn.cursor() as cur:
                return work(cur)
        except psycopg.OperationalError as err:
            if self._conn is not None:
                self._conn.close()
            self._conn = None
            raise SyncError(f"replica database: {err}") from err
        except psycopg.Error as err:
            raise SyncError(f"replica database: {type(err).__name__}: {err}") from err

    def load(self) -> State:
        def work(cur: psycopg.Cursor) -> State:
            cur.execute("""
                SELECT source_sync_uuid::TEXT, stream_schema_version, bootstrap_complete, rebuild_env_honoured,
                       accounts_block, accounts_after_name,
                       urls_block, urls_after_md5,
                       url_authors_block, url_authors_after_md5, url_authors_after_account,
                       profiles_block, profiles_after_account
                FROM proxy_whitelist_replica.state""")
            row = cur.fetchone()
            if row is None:
                raise SyncError("proxy_whitelist_replica.state has no row: run replica/install.sh")
            values = list(row[4:])
            cursors = {}
            for stream in STREAMS:
                block, values = values[0], values[1:]
                key, values = tuple(values[:len(stream.key_fields)]), values[len(stream.key_fields):]
                cursors[stream.name] = Cursor(block, None if key[0] is None else key)
            return State(row[0], row[1], cursors, row[2], row[3])
        return self._transaction(work)

    @staticmethod
    def _set_identity(cur: psycopg.Cursor, source_url: str, status: SourceStatus) -> None:
        cur.execute("""
            UPDATE proxy_whitelist_replica.state
            SET source_url = %s, source_sync_uuid = %s, stream_schema_version = %s, stream_sentinel_block = %s""",
                    (source_url, status.sync_uuid, status.stream_schema_version, status.stream_sentinel_block))

    def adopt_source(self, source_url: str, status: SourceStatus) -> None:
        self._transaction(lambda cur: self._set_identity(cur, source_url, status))

    def reset(self, source_url: str, status: SourceStatus, rebuild_env_honoured: bool) -> None:
        def work(cur: psycopg.Cursor) -> None:
            cur.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
                sql.SQL(", ").join(sql.SQL(t) for t in self._REPLICATED_TABLES)))
            # A new state row: fresh cursors, bootstrap_complete false and a newly minted replica_sync_uuid.
            cur.execute("DELETE FROM proxy_whitelist_replica.state RETURNING purge_enabled")
            row = cur.fetchone()
            cur.execute("INSERT INTO proxy_whitelist_replica.state (id, rebuild_env_honoured, purge_enabled) "
                        "VALUES (TRUE, %s, %s)", (rebuild_env_honoured, row is not None and row[0]))
            self._set_identity(cur, source_url, status)
        self._transaction(work)

    def set_rebuild_env_honoured(self, honoured: bool) -> None:
        self._transaction(lambda cur: cur.execute(
            "UPDATE proxy_whitelist_replica.state SET rebuild_env_honoured = %s", (honoured,)))

    def apply_page(self, stream: Stream, rows: list[dict[str, Any]], cursor: Cursor,
                   enqueue_purges: bool = False) -> int:
        """Applies rows and cursor in one transaction; returns how many accounts it queued for purging."""
        columns = [f"{stream.name}_block"] + [f"{stream.name}_{p}" for p in stream.after_params]
        values = [cursor.block] + list(cursor.key or (None,) * len(stream.after_params))
        update = sql.SQL("UPDATE proxy_whitelist_replica.state SET {}").format(
            sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in columns))

        def work(cur: psycopg.Cursor) -> int:
            queued = self._apply[stream.name](cur, rows, enqueue_purges) if rows else 0
            cur.execute(update, values)
            return queued
        return self._transaction(work)

    @staticmethod
    def _resolve_pending(cur: psycopg.Cursor) -> None:
        cur.execute("""
            WITH resolved AS (
                DELETE FROM proxy_whitelist_replica.pending_url_authors p
                USING proxy_whitelist_app.urls u, proxy_whitelist_app.accounts a
                WHERE md5(u.url) = p.url_md5 AND a.name = p.account_name
                RETURNING u.id AS url_id, a.id AS author_id, p.first_block
            )
            INSERT INTO proxy_whitelist_app.url_authors (url_id, author_id, first_block)
            SELECT url_id, author_id, first_block FROM resolved
            ON CONFLICT DO NOTHING""")

    def _apply_accounts(self, cur: psycopg.Cursor, rows: list[dict[str, Any]], _enqueue_purges: bool) -> int:
        cur.execute("""
            INSERT INTO proxy_whitelist_app.accounts (hive_id, name, first_block)
            SELECT * FROM unnest(%s::INT[], %s::TEXT[], %s::INT[])
            ON CONFLICT (hive_id) DO UPDATE SET name = EXCLUDED.name""",
                    ([r["hive_id"] for r in rows], [r["name"] for r in rows], [r["first_block"] for r in rows]))
        self._resolve_pending(cur)
        return 0

    def _apply_urls(self, cur: psycopg.Cursor, rows: list[dict[str, Any]], _enqueue_purges: bool) -> int:
        for r in rows:
            if hashlib.md5(r["url"].encode()).hexdigest() != r["md5"]:
                raise SyncError(f"url row whose md5 {r['md5']} does not match its url {r['url']!r}")
        cur.execute("""
            INSERT INTO proxy_whitelist_app.urls (url, first_block)
            SELECT * FROM unnest(%s::TEXT[], %s::INT[])
            ON CONFLICT ((md5(url))) DO NOTHING""",
                    ([r["url"] for r in rows], [r["first_block"] for r in rows]))
        self._resolve_pending(cur)
        return 0

    @staticmethod
    def _apply_url_authors(cur: psycopg.Cursor, rows: list[dict[str, Any]], _enqueue_purges: bool) -> int:
        cur.execute("""
            WITH input AS (
                SELECT * FROM unnest(%s::TEXT[], %s::TEXT[], %s::INT[]) AS i (url_md5, account_name, first_block)
            ),
            resolved AS (
                SELECT i.*, u.id AS url_id, a.id AS author_id
                FROM input i
                LEFT JOIN proxy_whitelist_app.urls u ON md5(u.url) = i.url_md5
                LEFT JOIN proxy_whitelist_app.accounts a ON a.name = i.account_name
            ),
            inserted AS (
                INSERT INTO proxy_whitelist_app.url_authors (url_id, author_id, first_block)
                SELECT url_id, author_id, first_block FROM resolved
                WHERE url_id IS NOT NULL AND author_id IS NOT NULL
                ON CONFLICT DO NOTHING
            )
            INSERT INTO proxy_whitelist_replica.pending_url_authors (url_md5, account_name, first_block)
            SELECT url_md5, account_name, first_block FROM resolved
            WHERE url_id IS NULL OR author_id IS NULL
            ON CONFLICT DO NOTHING""",
                    ([r["url_md5"] for r in rows], [r["account_name"] for r in rows], [r["first_block"] for r in rows]))
        return 0

    @staticmethod
    def _apply_profiles(cur: psycopg.Cursor, rows: list[dict[str, Any]], enqueue_purges: bool) -> int:
        # Every sub-statement sees the snapshot before the upsert, so `old` holds the previous urls.
        cur.execute("""
            WITH input AS (
                SELECT a.id AS account_id, i.*
                FROM unnest(%s::TEXT[], %s::TEXT[], %s::TEXT[], %s::INT[]) AS i (account_name, avatar_url, cover_url, updated_block)
                JOIN proxy_whitelist_app.accounts a ON a.name = i.account_name
            ),
            old AS (
                SELECT p.account_id, p.avatar_url, p.cover_url
                FROM proxy_whitelist_app.account_profiles p
                JOIN input i ON i.account_id = p.account_id
            ),
            upserted AS (
                INSERT INTO proxy_whitelist_app.account_profiles (account_id, avatar_url, cover_url, updated_block)
                SELECT account_id, avatar_url, cover_url, updated_block FROM input
                ON CONFLICT (account_id) DO UPDATE
                    SET avatar_url = EXCLUDED.avatar_url,
                        cover_url = EXCLUDED.cover_url,
                        updated_block = EXCLUDED.updated_block
                RETURNING account_id
            ),
            changed AS (
                SELECT i.account_name, i.updated_block,
                       i.avatar_url IS DISTINCT FROM o.avatar_url AS avatar_changed,
                       i.cover_url IS DISTINCT FROM o.cover_url AS cover_changed
                FROM input i
                LEFT JOIN old o ON o.account_id = i.account_id
            ),
            queued AS (
                INSERT INTO proxy_whitelist_replica.purge_queue AS q (account_name, block, avatar_changed, cover_changed)
                SELECT account_name, updated_block, avatar_changed, cover_changed FROM changed
                WHERE %s AND (avatar_changed OR cover_changed)
                ON CONFLICT (account_name) DO UPDATE
                    SET block = GREATEST(q.block, EXCLUDED.block),
                        avatar_changed = q.avatar_changed OR EXCLUDED.avatar_changed,
                        cover_changed = q.cover_changed OR EXCLUDED.cover_changed
                RETURNING account_name
            )
            SELECT (SELECT count(*) FROM upserted), (SELECT count(*) FROM queued)""",
                    ([r["account_name"] for r in rows], [r["avatar_url"] for r in rows],
                     [r["cover_url"] for r in rows], [r["updated_block"] for r in rows], enqueue_purges))
        row = cur.fetchone()
        upserted = row[0] if row else 0
        if upserted != len(rows):
            raise SyncError(f"{len(rows) - upserted} profile rows name accounts the replica does not have")
        return int(row[1]) if row else 0

    def replace_blacklists(self, urls: list[str], accounts: list[str]) -> None:
        def work(cur: psycopg.Cursor) -> tuple[int, int]:
            cur.execute("DELETE FROM proxy_whitelist_app.blacklisted_urls")
            cur.execute("""
                INSERT INTO proxy_whitelist_app.blacklisted_urls (url_id)
                SELECT u.id FROM unnest(%s::TEXT[]) AS b (url)
                JOIN proxy_whitelist_app.urls u ON md5(u.url) = md5(b.url) AND u.url = b.url
                ON CONFLICT DO NOTHING""", (urls,))
            found_urls = cur.rowcount
            cur.execute("DELETE FROM proxy_whitelist_app.blacklisted_accounts")
            cur.execute("""
                INSERT INTO proxy_whitelist_app.blacklisted_accounts (account_id)
                SELECT a.id FROM unnest(%s::TEXT[]) AS b (name)
                JOIN proxy_whitelist_app.accounts a ON a.name = b.name
                ON CONFLICT DO NOTHING""", (accounts,))
            return found_urls, cur.rowcount
        found_urls, found_accounts = self._transaction(work)
        if (found_urls, found_accounts) != (len(urls), len(accounts)):
            # A blacklisted row the streams have not delivered yet: the next cycle replaces the lists again.
            log.info("blacklists: %d of %d urls and %d of %d accounts are replicated yet",
                     found_urls, len(urls), found_accounts, len(accounts))

    def finish_cycle(self, source_url: str, status: SourceStatus) -> None:
        self._transaction(lambda cur: cur.execute("""
            UPDATE proxy_whitelist_replica.state
            SET source_url = %s, stream_sentinel_block = %s,
                source_block_num = %s, source_block_time = %s, polled_at = now(),
                sync_error = NULL, bootstrap_complete = TRUE""",
                                                  (source_url, status.stream_sentinel_block,
                                                   status.block_num, status.block_time)))

    def set_error(self, message: str) -> None:
        self._transaction(lambda cur: cur.execute(
            "UPDATE proxy_whitelist_replica.state SET sync_error = %s", (message,)))

    def set_purge_enabled(self, enabled: bool) -> None:
        self._transaction(lambda cur: cur.execute(
            "UPDATE proxy_whitelist_replica.state SET purge_enabled = %s", (enabled,)))

    def queued_purges(self) -> int:
        def work(cur: psycopg.Cursor) -> int:
            cur.execute("SELECT count(*) FROM proxy_whitelist_replica.purge_queue")
            row = cur.fetchone()
            return int(row[0]) if row else 0
        return self._transaction(work)

    def pending_purges(self, limit: int) -> list[PurgeItem]:
        def work(cur: psycopg.Cursor) -> list[PurgeItem]:
            cur.execute("""
                SELECT account_name, block, avatar_changed, cover_changed
                FROM proxy_whitelist_replica.purge_queue
                ORDER BY block, account_name
                LIMIT %s""", (limit,))
            return [PurgeItem(*row) for row in cur.fetchall()]
        return self._transaction(work)

    def purges_done(self, items: list[PurgeItem]) -> None:
        # Matched on the block too: a row a newer change raised since it was read stays queued.
        self._transaction(lambda cur: cur.execute("""
            DELETE FROM proxy_whitelist_replica.purge_queue q
            USING unnest(%s::TEXT[], %s::INT[]) AS d (account_name, block)
            WHERE q.account_name = d.account_name AND q.block = d.block""",
                                                  ([i.account_name for i in items], [i.block for i in items])))

    def purges_failed(self, items: list[PurgeItem], error: str) -> None:
        self._transaction(lambda cur: cur.execute("""
            UPDATE proxy_whitelist_replica.purge_queue
            SET attempts = attempts + 1, last_error = %s
            WHERE account_name = ANY(%s::TEXT[])""", (error, [i.account_name for i in items])))


def run(syncer: Syncer, poll_interval_s: float, once: bool,
        sleep: Callable[[float], None] = time.sleep) -> int:
    """Cycles until stopped: 0 after one successful --once cycle, 1 after a failed --once
    cycle, 2 after a --once cycle refused for a source identity mismatch without --reset.
    Without --once such a mismatch is checked again every poll_interval_s."""
    backoff = RETRY_SLEEP_S
    mismatch: str | None = None
    while True:
        try:
            status = syncer.cycle()
            log.debug("replicated up to block %s", status.block_num)
            if mismatch is not None:
                log.info("the source identity matches again; syncing resumed")
                mismatch = None
            backoff = RETRY_SLEEP_S
            if once:
                return 0
            sleep(poll_interval_s)
            continue
        except IdentityMismatch as err:
            if not syncer.reset:
                if once:
                    _record_error(syncer.store, err)
                    log.error("%s; refusing to continue (the replica keeps serving its data; "
                              "run with --reset to bootstrap again)", err)
                    return 2
                if str(err) != mismatch:
                    _record_error(syncer.store, err)
                    log.error("%s; writing nothing until it matches again, checking every %s s (the replica "
                              "keeps serving its data; run with --reset to bootstrap again)", err, poll_interval_s)
                    mismatch = str(err)
                sleep(poll_interval_s)
                continue
            _record_error(syncer.store, err)
            log.warning("%s", err)
        except SyncError as err:
            log.warning("%s", err)
            _record_error(syncer.store, err)
            mismatch = None
        if once:
            return 1
        log.info("retrying in %s s", backoff)
        sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_S)


def _record_error(store: Store, err: SyncError) -> None:
    try:
        store.set_error(str(err))
    except SyncError as store_err:
        log.warning("could not record sync_error: %s", store_err)


def _purger(store: PgStore) -> Purger | None:
    config = PurgeConfig.from_env(os.environ)
    if config is None:
        log.info("Cloudflare purging is off: PURGE_CLOUDFLARE_ZONE, PURGE_CLOUDFLARE_TOKEN and PURGE_BASE_URL "
                 "are not all set; profile changes are not queued")
        return None
    log.info("Cloudflare purging is on: zone %s, %d url templates under %s, %s s per drain",
             config.zone, len(config.url_templates), config.base_url, config.time_budget_s)
    return Purger(config, CloudflareClient(config), store)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--once", action="store_true", help="run one cycle and exit (1 when it fails)")
    parser.add_argument("--reset", action="store_true",
                        help="on a source identity change, truncate the replica and bootstrap again instead of waiting")
    parser.add_argument("--rebuild", action="store_true",
                        help="truncate the replica and bootstrap again before the first cycle, whatever the source identity")
    args = parser.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s  %(levelname)s  %(message)s")
    try:
        source_url = os.environ["PROXY_WHITELIST_SOURCE_URL"]
        postgres_url = os.environ["POSTGRES_URL"]
        poll_interval_s = float(os.environ.get("POLL_INTERVAL_S", "5"))
        page_limit = int(os.environ.get("PAGE_LIMIT", str(MAX_PAGE_SIZE)))
        store = PgStore(postgres_url)
        syncer = Syncer(HttpSource(source_url), store, source_url, page_limit,
                        reset=args.reset or os.environ.get("PROXY_WHITELIST_REPLICA_RESET") == "1",
                        rebuild=args.rebuild,
                        rebuild_from_env=os.environ.get("PROXY_WHITELIST_REPLICA_REBUILD") == "1",
                        purger=_purger(store))
    except KeyError as err:
        log.critical("missing environment variable %s", err)
        return 1
    except ValueError as err:
        log.critical("invalid configuration: %s", err)
        return 1
    return run(syncer, poll_interval_s, args.once)


if __name__ == "__main__":
    sys.exit(main())
