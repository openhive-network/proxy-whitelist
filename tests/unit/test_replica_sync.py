"""replica/sync.py's paging, cursors and source identity handling against an
in-memory source that serves stream contract v2 and an in-memory store, and
its HTTP client against that source answering in place of urllib. No
database, no network."""
import email.message
import gzip
import hashlib
import importlib.util
import io
import json
import logging
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
# sync.py imports its sibling purge.py, as it does when run as a script.
sys.path.insert(0, str(ROOT / "replica"))
_spec = importlib.util.spec_from_file_location("replica_sync", ROOT / "replica" / "sync.py")
assert _spec is not None and _spec.loader is not None
sync = importlib.util.module_from_spec(_spec)
sys.modules["replica_sync"] = sync
_spec.loader.exec_module(sync)
purge = importlib.import_module("purge")

UUID = "0b5f8f0e-6c3e-4a51-9a49-3f2f0c1d7e21"
OTHER_UUID = "7d3c2b1a-0000-4000-8000-000000000001"


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


class FakeSource:
    """The /stream/* pages of endpoints/stream.sql over in-memory rows, and their request log."""

    def __init__(self, head: int, uuid: str = UUID) -> None:
        self.head = head
        self.uuid = uuid
        self.page_uuid: str | None = None
        self.rows: dict[str, list[dict[str, Any]]] = {s.name: [] for s in sync.STREAMS}
        self.blacklisted_urls: list[str] = []
        self.blacklisted_accounts: list[str] = []
        self.requests: list[tuple[str, int, int, dict[str, str], int]] = []
        self.down = False

    def add_url(self, url: str, author: str, hive_id: int, block: int, author_block: int | None = None) -> None:
        if not any(a["name"] == author for a in self.rows["accounts"]):
            self.rows["accounts"].append({"first_block": block, "hive_id": hive_id, "name": author})
        if not any(u["url"] == url for u in self.rows["urls"]):
            self.rows["urls"].append({"first_block": block, "md5": md5(url), "url": url})
        self.rows["url_authors"].append(
            {"first_block": block if author_block is None else author_block, "url_md5": md5(url), "account_name": author})

    def set_profile(self, account: str, avatar: str | None, cover: str | None, block: int) -> None:
        self.rows["profiles"] = [p for p in self.rows["profiles"] if p["account_name"] != account]
        self.rows["profiles"].append(
            {"updated_block": block, "account_name": account, "avatar_url": avatar, "cover_url": cover})

    def status(self) -> Any:
        if self.down:
            raise sync.SyncError("GET /sync-status: connection refused")
        return sync.SourceStatus(self.head, datetime(2026, 11, 19, 12, 0, self.head % 60, tzinfo=timezone.utc),
                                 self.uuid, 2, 0)

    def page(self, stream: Any, from_block: int, to_block: int, after: dict[str, str], page_size: int) -> Any:
        if self.down:
            raise sync.SyncError(f"GET {stream.path}: connection refused")
        self.requests.append((stream.name, from_block, to_block, dict(after), page_size))
        assert from_block <= to_block, "the source answers 400 to to_block < from_block"
        applied = min(to_block, self.head)

        def order(r: dict[str, Any]) -> tuple[Any, ...]:
            return (r[stream.block_field],) + tuple(r[f] for f in stream.key_fields)
        start = (from_block,) + tuple(after[p] for p in stream.after_params) if after else None
        rows = sorted((r for r in self.rows[stream.name] if from_block <= r[stream.block_field] <= applied
                       and (start is None or order(r) > start)), key=order)[:page_size]
        to = rows[-1][stream.block_field] if len(rows) == page_size else applied
        return sync.Page([dict(r) for r in rows], to, self.page_uuid or self.uuid)

    def blacklists(self) -> tuple[list[str], list[str]]:
        return list(self.blacklisted_urls), list(self.blacklisted_accounts)


class FakeStore:
    """proxy_whitelist_replica.state and the replicated tables, keyed by natural keys."""

    def __init__(self) -> None:
        self._fresh()
        self.error: str | None = None
        self.errors_set = 0
        self.fail_on: str | None = None
        self.rebuild_env_honoured = False
        self.purge_enabled = False

    def _fresh(self) -> None:
        self.uuid: str | None = None
        self.version: int | None = None
        self.cursors = {s.name: sync.Cursor() for s in sync.STREAMS}
        self.accounts: dict[str, int] = {}
        self.urls: dict[str, str] = {}
        self.url_authors: set[tuple[str, str]] = set()
        self.pending: set[tuple[str, str]] = set()
        self.profiles: dict[str, tuple[Any, Any, int]] = {}
        self.blacklist: tuple[list[str], list[str]] = ([], [])
        self.bootstrap_complete = False
        self.source_block: int | None = None
        self.source_time: datetime | None = None
        self.resets = 0
        self.purge_queue: dict[str, dict[str, Any]] = {}

    def load(self) -> Any:
        return sync.State(self.uuid, self.version, dict(self.cursors), self.bootstrap_complete,
                          self.rebuild_env_honoured)

    def adopt_source(self, source_url: str, status: Any) -> None:
        self.uuid, self.version = status.sync_uuid, status.stream_schema_version

    def reset(self, source_url: str, status: Any, rebuild_env_honoured: bool) -> None:
        resets = self.resets
        self._fresh()
        self.resets = resets + 1
        self.rebuild_env_honoured = rebuild_env_honoured
        self.adopt_source(source_url, status)

    def set_rebuild_env_honoured(self, honoured: bool) -> None:
        self.rebuild_env_honoured = honoured

    def apply_page(self, stream: Any, rows: list[dict[str, Any]], cursor: Any, enqueue_purges: bool = False) -> int:
        if self.fail_on == stream.name:
            raise sync.SyncError("replica database: constraint violation")
        queued = 0
        for r in rows:
            if stream is sync.PROFILES and enqueue_purges:
                queued += self._enqueue(r)
            if stream is sync.ACCOUNTS:
                self.accounts[r["name"]] = r["hive_id"]
            elif stream is sync.URLS:
                self.urls[r["md5"]] = r["url"]
            elif stream is sync.URL_AUTHORS:
                self.pending.add((r["url_md5"], r["account_name"]))
            else:
                self.profiles[r["account_name"]] = (r["avatar_url"], r["cover_url"], r["updated_block"])
        for key in list(self.pending):
            if key[0] in self.urls and key[1] in self.accounts:
                self.pending.discard(key)
                self.url_authors.add(key)
        self.cursors[stream.name] = cursor
        return queued

    def _enqueue(self, r: dict[str, Any]) -> int:
        old_avatar, old_cover, _ = self.profiles.get(r["account_name"], (None, None, None))
        avatar, cover = r["avatar_url"] != old_avatar, r["cover_url"] != old_cover
        if not (avatar or cover):
            return 0
        q = self.purge_queue.setdefault(r["account_name"], {"block": r["updated_block"], "avatar_changed": False,
                                                            "cover_changed": False, "attempts": 0, "last_error": None})
        q["block"] = max(q["block"], r["updated_block"])
        q["avatar_changed"] |= avatar
        q["cover_changed"] |= cover
        return 1

    def pending_purges(self, limit: int) -> list[Any]:
        items = [purge.PurgeItem(name, q["block"], q["avatar_changed"], q["cover_changed"])
                 for name, q in self.purge_queue.items()]
        return sorted(items, key=lambda i: (i.block, i.account_name))[:limit]

    def purges_done(self, items: list[Any]) -> None:
        for i in items:
            if self.purge_queue.get(i.account_name, {}).get("block") == i.block:
                del self.purge_queue[i.account_name]

    def purges_failed(self, items: list[Any], error: str) -> None:
        for i in items:
            self.purge_queue[i.account_name]["attempts"] += 1
            self.purge_queue[i.account_name]["last_error"] = error

    def set_purge_enabled(self, enabled: bool) -> None:
        self.purge_enabled = enabled

    def replace_blacklists(self, urls: list[str], accounts: list[str]) -> None:
        self.blacklist = (urls, accounts)

    def finish_cycle(self, source_url: str, status: Any) -> None:
        self.source_block, self.source_time = status.block_num, status.block_time
        self.error = None
        self.bootstrap_complete = True

    def set_error(self, message: str) -> None:
        self.error = message
        self.errors_set += 1


SOURCE_URL = "http://primary.invalid/proxy-whitelist-api"


class HttpFixture:
    """A FakeSource behind the primary's URL rewriter, answering HttpSource's urllib requests.

    failures[stream] are raised, one per request, before that stream's next pages are
    served; bodies[stream] replaces the body of its pages, sent with body_headers. With
    gzip set, answers to a request accepting gzip are compressed, as caddy's encode
    directive does."""

    def __init__(self, source: FakeSource) -> None:
        self.source = source
        self.failures: dict[str, list[Exception]] = {}
        self.bodies: dict[str, bytes] = {}
        self.body_headers: dict[str, str] = {}
        self.gzip = False
        self.accept_headers: set[tuple[str | None, str | None]] = set()
        self.page_requests: list[tuple[str, int, dict[str, str], int, str]] = []

    def urlopen(self, req: urllib.request.Request, timeout: float) -> "Response":
        url = urllib.parse.urlsplit(req.full_url)
        path = url.path.removeprefix(urllib.parse.urlsplit(SOURCE_URL).path)
        query = dict(urllib.parse.parse_qsl(url.query))
        self.accept_headers.add((req.get_header("Accept"), req.get_header("Accept-encoding")))
        compress = self.gzip and "gzip" in (req.get_header("Accept-encoding") or "")
        if path == "/sync-status":
            status = self.source.status()
            return self._answer(json.dumps({
                "last_block_num": status.block_num, "last_block_time": status.block_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "sync_uuid": status.sync_uuid, "stream_schema_version": status.stream_schema_version,
                "stream_sentinel_block": status.stream_sentinel_block}).encode(), compress)
        if path == "/stream/blacklists":
            urls, accounts = self.source.blacklists()
            return self._answer(json.dumps({"urls": urls, "accounts": accounts}).encode(), compress)
        stream = next(s for s in sync.STREAMS if s.path == path)
        after = {p: query[p] for p in stream.after_params if p in query}
        failures = self.failures.get(stream.name, [])
        outcome = type(failures[0]).__name__ if failures else "ok"
        self.page_requests.append((stream.name, int(query["from_block"]), after, int(query["page_size"]), outcome))
        if failures:
            raise failures.pop(0)
        page = self.source.page(stream, int(query["from_block"]), int(query["to_block"]), after, int(query["page_size"]))
        headers = {"Stream-To-Block": str(page.to_block), "Stream-Sync-Uuid": page.sync_uuid}
        if stream.name in self.bodies:
            return Response(self.bodies[stream.name], {"Content-Type": "application/json", **headers, **self.body_headers})
        return self._answer(json.dumps(page.rows).encode(), compress, headers)

    @staticmethod
    def _answer(body: bytes, compress: bool, headers: dict[str, str] | None = None) -> "Response":
        headers = {"Content-Type": "application/json", **(headers or {})}
        if compress:
            body, headers["Content-Encoding"] = gzip.compress(body), "gzip"
        return Response(body, headers)


class Response(io.BytesIO):
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        super().__init__(body)
        self.headers = headers


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(SOURCE_URL, code, "Gateway Time-out", email.message.Message(),
                                  io.BytesIO(b"<html><body><h1>504 Gateway Time-out</h1></body></html>"))


@pytest.fixture
def http(monkeypatch: pytest.MonkeyPatch) -> HttpFixture:
    fixture = HttpFixture(seeded_source())
    monkeypatch.setattr(sync.urllib.request, "urlopen", fixture.urlopen)
    return fixture


def syncer(source: Any, store: FakeStore, page_size: int = 100, reset: bool = False, **kwargs: Any) -> Any:
    return sync.Syncer(source, store, SOURCE_URL, page_size, reset, **kwargs)


def seeded_source() -> FakeSource:
    source = FakeSource(head=20)
    source.add_url("https://a.invalid/1.png", "alice", 1, 10)
    source.add_url("https://a.invalid/2.png", "bob", 2, 12)
    source.set_profile("alice", "https://a.invalid/avatar.png", None, 15)
    source.blacklisted_urls = ["https://a.invalid/2.png"]
    return source


def test_bootstrap_replicates_every_stream_up_to_the_source_head() -> None:
    source, store = seeded_source(), FakeStore()
    status = syncer(source, store).cycle()

    assert status.block_num == 20
    assert store.bootstrap_complete and store.source_block == 20 and store.source_time == source.status().block_time
    assert store.uuid == UUID
    assert set(store.urls.values()) == {"https://a.invalid/1.png", "https://a.invalid/2.png"}
    assert store.url_authors == {(md5("https://a.invalid/1.png"), "alice"), (md5("https://a.invalid/2.png"), "bob")}
    assert store.profiles == {"alice": ("https://a.invalid/avatar.png", None, 15)}
    assert store.blacklist == (["https://a.invalid/2.png"], [])
    assert all(c == sync.Cursor(20) for c in store.cursors.values())
    # Bootstrap starts at block 0 with no tiebreak; one short page completes each stream.
    assert [r[:4] for r in source.requests] == [(s.name, 0, 20, {}) for s in sync.STREAMS]


def test_full_pages_continue_from_the_last_row_and_a_short_page_completes_the_range() -> None:
    source, store = FakeSource(head=7), FakeStore()
    urls = [f"https://a.invalid/page-{i}.png" for i in range(5)]
    for url in urls:
        source.add_url(url, "alice", 1, 5)
    syncer(source, store, page_size=2).cycle()

    assert sorted(store.urls) == sorted(md5(u) for u in urls)
    url_requests = [r for r in source.requests if r[0] == "urls"]
    first, last = sorted(md5(u) for u in urls)[1], sorted(md5(u) for u in urls)[3]
    assert url_requests == [
        ("urls", 0, 7, {}, 2),
        ("urls", 5, 7, {"after_md5": first}, 2),
        ("urls", 5, 7, {"after_md5": last}, 2),
    ]
    assert store.cursors["urls"] == sync.Cursor(7)
    author_requests = [r for r in source.requests if r[0] == "url_authors"]
    assert author_requests[1][3] == {"after_md5": first, "after_account": "alice"}
    assert len(store.url_authors) == 5


def test_a_full_last_page_is_followed_by_an_empty_page_that_completes_the_range() -> None:
    source, store = FakeSource(head=9), FakeStore()
    for i in range(2):
        source.add_url(f"https://a.invalid/{i}.png", "alice", 1, 5)
    syncer(source, store, page_size=2).cycle()

    url_requests = [r[1:4] for r in source.requests if r[0] == "urls"]
    assert url_requests == [(0, 9, {}), (5, 9, {"after_md5": max(md5(f"https://a.invalid/{i}.png") for i in range(2))})]
    assert store.cursors["urls"] == sync.Cursor(9)


def test_cycles_without_new_rows_still_advance_to_the_source_head() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.requests.clear()
    source.head = 25
    s.cycle()

    assert store.source_block == 25
    assert all(c == sync.Cursor(25) for c in store.cursors.values())
    assert [r[1:3] for r in source.requests] == [(21, 25)] * 4
    source.requests.clear()
    s.cycle()
    assert source.requests == [], "a stream already at the head is not requested again"
    assert store.source_block == 25


def test_new_rows_after_bootstrap_are_replicated_in_the_next_cycle() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.head = 21
    source.add_url("https://a.invalid/new.png", "carol", 3, 21)
    source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    s.cycle()

    assert (md5("https://a.invalid/new.png"), "carol") in store.url_authors
    assert store.profiles["alice"] == ("https://a.invalid/avatar2.png", None, 21)
    assert store.source_block == 21


def test_a_url_author_below_its_url_block_waits_in_pending_until_the_url_arrives() -> None:
    source, store = FakeSource(head=10), FakeStore()
    source.add_url("https://a.invalid/1.png", "alice", 1, 15, author_block=8)
    s = syncer(source, store)
    s.cycle()
    assert store.pending == {(md5("https://a.invalid/1.png"), "alice")}
    assert store.url_authors == set()

    source.head = 15
    s.cycle()
    assert store.pending == set()
    assert store.url_authors == {(md5("https://a.invalid/1.png"), "alice")}


def test_a_source_identity_change_is_refused_without_writing_rows() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.uuid = OTHER_UUID
    source.head = 30
    source.add_url("https://a.invalid/after.png", "carol", 3, 30)
    source.requests.clear()

    assert sync.run(s, 5, once=True) == 2
    assert store.error is not None and OTHER_UUID in store.error
    assert md5("https://a.invalid/after.png") not in store.urls
    assert source.requests == []
    assert store.source_block == 20 and store.uuid == UUID


def test_reset_truncates_and_bootstraps_again_from_the_new_source() -> None:
    source, store = seeded_source(), FakeStore()
    syncer(source, store).cycle()
    store.accounts["stale"] = 99
    source.uuid = OTHER_UUID
    source.requests.clear()

    assert sync.run(syncer(source, store, reset=True), 5, once=True) == 0
    assert store.resets == 1
    assert store.uuid == OTHER_UUID
    assert "stale" not in store.accounts
    assert len(store.urls) == 2 and store.bootstrap_complete
    assert source.requests[0][1] == 0, "the bootstrap starts again at block 0"


def test_a_page_from_another_source_identity_is_not_applied() -> None:
    source, store = seeded_source(), FakeStore()
    source.page_uuid = OTHER_UUID
    with pytest.raises(sync.IdentityMismatch):
        syncer(source, store).cycle()
    assert store.accounts == {} and store.cursors["accounts"] == sync.Cursor()
    assert not store.bootstrap_complete


def test_an_unsupported_stream_schema_version_is_refused_on_first_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    source, store = seeded_source(), FakeStore()
    monkeypatch.setattr(source, "status", lambda: sync.SourceStatus(20, None, UUID, 3, 0))
    assert sync.run(syncer(source, store, reset=True), 5, once=True) == 1
    assert store.uuid is None
    assert store.error is not None and "stream_schema_version 3" in store.error


def test_an_unreachable_source_sets_sync_error_and_backs_off_then_resumes_from_the_cursors() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.down = True
    source.head = 22
    source.add_url("https://a.invalid/late.png", "alice", 1, 22)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 4:
            source.down = False
        if len(sleeps) == 6:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        sync.run(s, 5, once=False, sleep=sleep)
    assert sleeps == [3, 6, 12, 24, 5, 5]
    assert store.error is None
    assert store.source_block == 22
    assert len(store.urls) == 3 and len(store.url_authors) == 3


def test_a_failed_page_keeps_the_cursor_and_records_the_stream() -> None:
    source, store = seeded_source(), FakeStore()
    store.fail_on = "urls"
    assert sync.run(syncer(source, store), 5, once=True) == 1
    assert store.error is not None and store.error.startswith("urls at cursor -1:")
    assert store.cursors["accounts"] == sync.Cursor(20)
    assert store.cursors["urls"] == sync.Cursor()
    assert not store.bootstrap_complete and store.source_block is None

    store.fail_on = None
    assert sync.run(syncer(source, store), 5, once=True) == 0
    assert len(store.urls) == 2 and store.error is None


def test_a_short_page_that_stops_below_the_requested_block_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    source, store = seeded_source(), FakeStore()
    real_page = source.page
    monkeypatch.setattr(source, "page", lambda *a: sync.Page(real_page(*a).rows, 19, UUID))
    with pytest.raises(sync.SyncError, match="short page with Stream-To-Block 19"):
        syncer(source, store).cycle()
    assert store.cursors["accounts"] == sync.Cursor()


def test_a_source_head_below_the_replicated_block_is_an_error() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.head = 15
    with pytest.raises(sync.SyncError, match="below the replicated block"):
        s.cycle()
    assert store.source_block == 20


@pytest.mark.parametrize("page_size", [0, sync.MAX_PAGE_SIZE + 1])
def test_page_size_outside_the_source_cap_is_rejected(page_size: int) -> None:
    with pytest.raises(ValueError):
        syncer(FakeSource(head=1), FakeStore(), page_size=page_size)


def test_a_source_identity_change_without_once_waits_and_resumes_when_the_identity_returns() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store)
    s.cycle()
    source.uuid = OTHER_UUID
    source.head = 30
    source.add_url("https://a.invalid/after.png", "carol", 3, 30)
    source.requests.clear()
    sleeps: list[tuple[float, int | None, str | None]] = []

    def sleep(seconds: float) -> None:
        sleeps.append((seconds, store.source_block, store.error))
        if len(sleeps) == 3:
            source.uuid = UUID
        if len(sleeps) == 4:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        sync.run(s, 5, once=False, sleep=sleep)
    assert [t[0] for t in sleeps] == [5, 5, 5, 5], "the identity is checked again every poll interval, no backoff"
    for _, block, error in sleeps[:3]:
        assert block == 20
        assert error is not None and OTHER_UUID in error and UUID in error
    assert store.errors_set == 1, "sync_error is written once, not on every check"
    assert sleeps[3][1:] == (30, None), "the first poll after the identity returned replicates and clears sync_error"
    assert md5("https://a.invalid/after.png") in store.urls
    assert store.resets == 0 and store.uuid == UUID


def test_rebuild_truncates_and_bootstraps_again_from_an_unchanged_source(monkeypatch: pytest.MonkeyPatch) -> None:
    source, store = seeded_source(), FakeStore()
    syncer(source, store).cycle()
    # cli/blacklist.py remove-all on the primary: deletions are not streamed.
    source.rows["url_authors"] = [r for r in source.rows["url_authors"] if r["account_name"] != "bob"]
    source.rows["urls"] = [r for r in source.rows["urls"] if r["url"] != "https://a.invalid/2.png"]
    source.blacklisted_urls = []
    syncer(source, store).cycle()
    assert md5("https://a.invalid/2.png") in store.urls
    source.requests.clear()
    bootstrap_complete_while_applying: list[bool] = []
    apply_page = store.apply_page

    def recording_apply_page(*args: Any) -> None:
        bootstrap_complete_while_applying.append(store.bootstrap_complete)
        apply_page(*args)
    monkeypatch.setattr(store, "apply_page", recording_apply_page)

    assert sync.run(syncer(source, store, rebuild=True), 5, once=True) == 0
    assert store.resets == 1 and store.uuid == UUID
    assert bootstrap_complete_while_applying == [False] * 4
    assert set(store.urls.values()) == {"https://a.invalid/1.png"}
    assert store.url_authors == {(md5("https://a.invalid/1.png"), "alice")}
    assert store.bootstrap_complete and store.source_block == 20
    assert [r[1] for r in source.requests] == [0] * 4, "every stream starts again at block 0"


def test_the_rebuild_environment_variable_is_honoured_once_until_it_is_seen_unset() -> None:
    source, store = seeded_source(), FakeStore()
    syncer(source, store).cycle()

    def start(rebuild_env: bool) -> None:
        assert sync.run(syncer(source, store, rebuild_from_env=rebuild_env), 5, once=True) == 0

    start(rebuild_env=True)
    assert store.resets == 1 and store.rebuild_env_honoured
    start(rebuild_env=True)
    assert store.resets == 1, "a restart with the variable still set does not rebuild again"
    start(rebuild_env=False)
    assert store.resets == 1 and not store.rebuild_env_honoured
    start(rebuild_env=True)
    assert store.resets == 2
    assert len(store.urls) == 2 and store.bootstrap_complete


def test_a_rebuild_happens_once_per_process_not_every_cycle() -> None:
    source, store = seeded_source(), FakeStore()
    s = syncer(source, store, rebuild=True)
    s.cycle()
    source.head = 21
    s.cycle()
    assert store.resets == 1 and store.source_block == 21


@pytest.mark.parametrize("failure", [lambda: http_error(504), lambda: http_error(502), lambda: TimeoutError("timed out"),
                                     lambda: urllib.error.URLError(TimeoutError("timed out"))],
                         ids=["504", "502", "read-timeout", "connect-timeout"])
def test_a_page_the_source_path_gives_up_on_is_retried_with_half_the_page_size(
        http: HttpFixture, failure: Any, caplog: pytest.LogCaptureFixture) -> None:
    store = FakeStore()
    s = syncer(sync.HttpSource(SOURCE_URL), store, page_size=4000)
    http.failures["urls"] = [failure()]
    with caplog.at_level(logging.INFO, logger="proxy_whitelist_replica"):
        assert sync.run(s, 5, once=True) == 0

    url_requests = [r for r in http.page_requests if r[0] == "urls"]
    assert [(r[1], r[2], r[3]) for r in url_requests] == [(0, {}, 4000), (0, {}, 2000)], \
        "the retry starts from the same cursor"
    assert url_requests[0][4] != "ok" and url_requests[1][4] == "ok"
    assert store.error is None and len(store.urls) == 2 and store.cursors["urls"] == sync.Cursor(20)
    assert "urls: " in caplog.text and "retrying with page_size 2000 instead of 4000" in caplog.text
    assert "urls: page_size 2000 passed; back to 4000" in caplog.text

    http.source.head = 21
    s.cycle()
    assert [r[3] for r in http.page_requests if r[0] == "urls"][-1] == 4000


def test_a_page_still_failing_at_the_minimum_page_size_is_a_sync_error_that_keeps_the_cursor(http: HttpFixture) -> None:
    store = FakeStore()
    http.failures["urls"] = [http_error(504) for _ in range(4)]
    assert sync.run(syncer(sync.HttpSource(SOURCE_URL), store, page_size=4000), 5, once=True) == 1

    assert [r[3] for r in http.page_requests if r[0] == "urls"] == [4000, 2000, sync.MIN_PAGE_SIZE]
    assert store.error is not None and store.error.startswith("urls at cursor -1:") and "HTTP 504" in store.error
    assert store.cursors["urls"] == sync.Cursor() and store.cursors["accounts"] == sync.Cursor(20)


def test_a_page_size_below_the_minimum_is_not_halved(http: HttpFixture) -> None:
    store = FakeStore()
    http.failures["urls"] = [http_error(504)]
    assert sync.run(syncer(sync.HttpSource(SOURCE_URL), store, page_size=100), 5, once=True) == 1
    assert [r[3] for r in http.page_requests if r[0] == "urls"] == [100]


ODD_URL = "https://a.invalid/TDGWlLc8-IMG\\_7570s \"q\"\t\u00e9.jpg"
ODD_AVATAR = "https://a.invalid/avatar\\_1.png"


def test_pages_are_read_as_json_with_every_value_exact(http: HttpFixture) -> None:
    store = FakeStore()
    http.source.add_url(ODD_URL, "alice", 1, 18)
    http.source.set_profile("alice", ODD_AVATAR, None, 19)
    assert sync.run(syncer(sync.HttpSource(SOURCE_URL), store), 5, once=True) == 0

    assert ODD_URL.count("\\") == 1 and store.urls[md5(ODD_URL)] == ODD_URL
    assert all(md5(url) == key for key, url in store.urls.items())
    assert store.profiles["alice"] == (ODD_AVATAR, None, 19)
    assert {accept for accept, _ in http.accept_headers} == {"application/json"}


@pytest.mark.parametrize("compressed", [True, False], ids=["gzip", "identity"])
def test_a_compressed_source_yields_the_same_rows_and_the_first_page_logs_whether_it_compresses(
        http: HttpFixture, compressed: bool, caplog: pytest.LogCaptureFixture) -> None:
    http.gzip = compressed
    http.source.add_url(ODD_URL, "alice", 1, 18)
    store, expected = FakeStore(), FakeStore()
    syncer(http.source, expected).cycle()
    with caplog.at_level(logging.INFO, logger="proxy_whitelist_replica"):
        assert sync.run(syncer(sync.HttpSource(SOURCE_URL), store), 5, once=True) == 0

    assert (store.accounts, store.urls, store.url_authors, store.profiles, store.blacklist) == \
        (expected.accounts, expected.urls, expected.url_authors, expected.profiles, expected.blacklist)
    assert {encoding for _, encoding in http.accept_headers} == {"gzip"}
    compression_lines = [r.getMessage() for r in caplog.records if "compress" in r.getMessage()]
    assert compression_lines == ["the source compresses stream pages (Content-Encoding: gzip)" if compressed
                                 else "the source does not compress stream pages; they arrive uncompressed"]


@pytest.mark.parametrize("body", [lambda b: gzip.compress(b)[:-12], lambda b: b"\x1f\x8b not gzip at all"],
                         ids=["truncated", "corrupt"])
def test_an_undecodable_gzip_page_is_a_sync_error_with_its_stream_and_cursor_and_retried(
        http: HttpFixture, body: Any) -> None:
    store = FakeStore()
    http.bodies["urls"] = body(json.dumps(http.source.page(sync.URLS, 0, 20, {}, 100).rows).encode())
    http.body_headers = {"Content-Encoding": "gzip"}
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 1:
            assert store.error is not None and store.error.startswith("urls at cursor -1:")
            assert "undecodable gzip body" in store.error
            assert store.cursors["urls"] == sync.Cursor() and store.cursors["accounts"] == sync.Cursor(20)
            del http.bodies["urls"]
        else:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        sync.run(syncer(sync.HttpSource(SOURCE_URL), store), 5, once=False, sleep=sleep)
    assert sleeps == [sync.RETRY_SLEEP_S, 5]
    assert store.error is None and len(store.urls) == 2


CF_API = "https://cf.invalid/client/v4"
ZONE, TOKEN, BASE = "zone-1", "secret-token-value", "https://images.invalid"
AVATAR_PATHS = ["avatar", "avatar/small", "avatar/medium", "avatar/large"]


def avatar_urls(name: str) -> set[str]:
    return {f"{BASE}{prefix}/u/{name}/{p}" for prefix in ("", "/webp") for p in AVATAR_PATHS}


def cover_urls(name: str) -> set[str]:
    return {f"{BASE}/u/{name}/cover", f"{BASE}/webp/u/{name}/cover"}


class Cloudflare:
    """Cloudflare's purge_cache answering CloudflareClient's urllib requests, and its call log.

    answers are consumed one per call: an exception is raised, an int is the HTTP status
    (429 with Retry-After: retry_after); with none left a call succeeds. Each call records
    its files and the store's queue and profiles as they were when it arrived."""

    def __init__(self, store: FakeStore) -> None:
        self.store = store
        self.calls: list[dict[str, Any]] = []
        self.answers: list[Any] = []
        self.retry_after = "7"

    def urlopen(self, req: urllib.request.Request, timeout: float) -> Response:
        assert req.full_url == f"{CF_API}/zones/{ZONE}/purge_cache" and req.get_method() == "POST"
        assert req.get_header("Authorization") == f"Bearer {TOKEN}"
        assert isinstance(req.data, bytes)
        self.calls.append({"files": json.loads(req.data)["files"],
                           "queue": {k: dict(v) for k, v in self.store.purge_queue.items()},
                           "profiles": dict(self.store.profiles)})
        answer = self.answers.pop(0) if self.answers else 200
        if isinstance(answer, Exception):
            raise answer
        if answer != 200:
            headers = email.message.Message()
            if answer == 429:
                headers["Retry-After"] = self.retry_after
            raise urllib.error.HTTPError(req.full_url, answer, "error", headers, io.BytesIO(b'{"success":false}'))
        return Response(b'{"success":true,"errors":[],"messages":[],"result":{"id":"x"}}', {})

    def files(self, call: int = -1) -> set[str]:
        return set(self.calls[call]["files"])


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


def purge_config(**kwargs: Any) -> Any:
    return purge.PurgeConfig(zone=ZONE, token=TOKEN, base_url=BASE, api_url=CF_API, **kwargs)


def purging(monkeypatch: pytest.MonkeyPatch, store: FakeStore, clock: Clock, **kwargs: Any) -> tuple[Any, Cloudflare]:
    cloudflare = Cloudflare(store)
    monkeypatch.setattr(purge.urllib.request, "urlopen", cloudflare.urlopen)
    config = purge_config(**kwargs)
    return purge.Purger(config, purge.CloudflareClient(config), store, clock), cloudflare


def bootstrapped(monkeypatch: pytest.MonkeyPatch, clock: Clock) -> tuple[FakeSource, FakeStore, Any, Cloudflare]:
    source, store = seeded_source(), FakeStore()
    purger, cloudflare = purging(monkeypatch, store, clock)
    s = syncer(source, store, purger=purger)
    s.cycle()
    assert store.bootstrap_complete and store.profiles and store.purge_queue == {} and cloudflare.calls == [], \
        "the bootstrap's profiles are not purged"
    assert store.purge_enabled
    return source, store, s, cloudflare


def queued(block: int, avatar: bool, cover: bool, attempts: int = 0, error: str | None = None) -> dict[str, Any]:
    return {"block": block, "avatar_changed": avatar, "cover_changed": cover, "attempts": attempts, "last_error": error}


def test_a_changed_avatar_is_queued_with_the_profile_and_purged_after_it_is_applied(
        monkeypatch: pytest.MonkeyPatch, clock: Clock) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    source.head = 21
    source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    s.cycle()

    assert len(cloudflare.calls) == 1
    assert sorted(cloudflare.calls[0]["files"]) == sorted(avatar_urls("alice"))
    assert cloudflare.calls[0]["queue"] == {"alice": queued(21, True, False)}
    assert cloudflare.calls[0]["profiles"]["alice"][0] == "https://a.invalid/avatar2.png", \
        "the purge follows the applied profile"
    assert store.purge_queue == {} and store.error is None


def test_a_profile_row_with_the_same_urls_is_not_queued_or_purged(
        monkeypatch: pytest.MonkeyPatch, clock: Clock) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    source.head = 21
    source.set_profile("alice", "https://a.invalid/avatar.png", None, 21)
    s.cycle()

    assert store.profiles["alice"] == ("https://a.invalid/avatar.png", None, 21)
    assert store.purge_queue == {} and cloudflare.calls == []


@pytest.mark.parametrize("change, expected", [
    (("alice", None, None), avatar_urls("alice")),
    (("alice", "https://a.invalid/avatar.png", "https://a.invalid/cover.png"), cover_urls("alice")),
    (("bob", "https://a.invalid/bob.png", None), avatar_urls("bob")),
], ids=["avatar-removed", "cover-set", "first-profile"])
def test_a_url_set_or_removed_purges_exactly_what_changed(
        monkeypatch: pytest.MonkeyPatch, clock: Clock, change: tuple[str, Any, Any], expected: set[str]) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    source.head = 21
    source.set_profile(*change, 21)
    s.cycle()

    assert len(cloudflare.calls) == 1 and cloudflare.files() == expected
    assert store.purge_queue == {}


@pytest.mark.parametrize("restart", [{"rebuild": True}, {"reset": True}], ids=["rebuild", "reset"])
def test_rebuild_and_reset_empty_the_queue_and_purge_nothing_they_apply(
        monkeypatch: pytest.MonkeyPatch, clock: Clock, restart: dict[str, bool]) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    cloudflare.answers = [http_error(503)]
    source.head = 21
    source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    s.cycle()
    assert set(store.purge_queue) == {"alice"}
    if restart.get("reset"):
        source.uuid = OTHER_UUID
    source.set_profile("bob", "https://a.invalid/bob.png", None, 21)
    calls = len(cloudflare.calls)
    clock.now += 3600
    config = purge_config()

    restarted = syncer(source, store, purger=purge.Purger(config, purge.CloudflareClient(config), store, clock), **restart)
    assert sync.run(restarted, 5, once=True) == 0
    assert store.resets == 1 and store.bootstrap_complete and store.purge_enabled
    assert store.profiles["bob"][0] == "https://a.invalid/bob.png"
    assert store.purge_queue == {} and len(cloudflare.calls) == calls


@pytest.mark.parametrize("failure", [lambda: http_error(503), lambda: urllib.error.URLError(ConnectionRefusedError()),
                                     lambda: purge.http.client.IncompleteRead(b"{"), lambda: 429],
                         ids=["503", "connection-refused", "truncated-answer", "429"])
def test_a_failed_purge_keeps_the_rows_and_sync_goes_on_until_a_later_drain_purges_them(
        monkeypatch: pytest.MonkeyPatch, clock: Clock, failure: Any) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    cloudflare.answers = [failure()]
    source.head = 21
    source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    assert sync.run(s, 5, once=True) == 0

    assert len(cloudflare.calls) == 1, "the drain stops at the failed call, the end-of-cycle drain waits"
    assert store.purge_queue["alice"]["attempts"] == 1 and store.purge_queue["alice"]["last_error"]
    assert store.error is None and store.source_block == 21

    source.head = 22
    source.add_url("https://a.invalid/later.png", "carol", 3, 22)
    s.cycle()
    assert store.cursors["urls"] == sync.Cursor(22) and store.error is None
    assert len(cloudflare.calls) == 1, "still within the purge backoff"

    clock.now += 7
    source.head = 23
    s.cycle()
    assert len(cloudflare.calls) == 2 and cloudflare.files() == avatar_urls("alice")
    assert store.purge_queue == {} and store.error is None


def test_the_purge_backoff_doubles_up_to_its_cap_and_a_429_waits_its_retry_after(
        monkeypatch: pytest.MonkeyPatch, clock: Clock) -> None:
    store = FakeStore()
    purger, cloudflare = purging(monkeypatch, store, clock)
    store.purge_queue["alice"] = queued(5, True, False)
    cloudflare.answers = [http_error(503) for _ in range(9)] + [429]
    times = []
    while len(cloudflare.calls) < 10:
        calls = len(cloudflare.calls)
        purger.drain()
        if len(cloudflare.calls) > calls:
            times.append(clock.now)
        clock.now += 1
    assert [b - a for a, b in zip(times, times[1:])] == [3, 6, 12, 24, 48, 96, 192, 300, 300]
    assert store.purge_queue["alice"]["attempts"] == 10 and "HTTP 429" in store.purge_queue["alice"]["last_error"]

    clock.now = times[-1] + 6.9
    purger.drain()
    assert len(cloudflare.calls) == 10, "Retry-After: 7 is honoured"
    clock.now = times[-1] + 7
    purger.drain()
    assert len(cloudflare.calls) == 11 and store.purge_queue == {}


def test_two_changes_before_the_purge_are_one_row_and_one_purge(
        monkeypatch: pytest.MonkeyPatch, clock: Clock) -> None:
    source, store, s, cloudflare = bootstrapped(monkeypatch, clock)
    cloudflare.answers = [http_error(503)]
    source.head = 21
    source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    s.cycle()
    error = store.purge_queue["alice"]["last_error"]
    source.head = 22
    source.set_profile("alice", "https://a.invalid/avatar2.png", "https://a.invalid/cover2.png", 22)
    s.cycle()
    assert store.purge_queue == {"alice": queued(22, True, True, 1, error)}

    clock.now += 3
    s.cycle()
    assert len(cloudflare.calls) == 2
    assert cloudflare.files() == avatar_urls("alice") | cover_urls("alice")
    assert store.purge_queue == {}


def test_a_drain_sends_at_most_30_urls_per_call_and_stops_at_its_time_budget(
        monkeypatch: pytest.MonkeyPatch, clock: Clock) -> None:
    store = FakeStore()
    purger, cloudflare = purging(monkeypatch, store, clock, time_budget_s=5)
    for i in range(10):
        store.purge_queue[f"user{i}"] = queued(i, True, True)
    urlopen = cloudflare.urlopen

    def slow_urlopen(req: urllib.request.Request, timeout: float) -> Response:
        clock.now += 2
        return urlopen(req, timeout)
    monkeypatch.setattr(purge.urllib.request, "urlopen", slow_urlopen)
    purger.drain()

    assert [len(c["files"]) for c in cloudflare.calls] == [30, 30, 30]
    assert cloudflare.files(0) == set().union(*(avatar_urls(f"user{i}") | cover_urls(f"user{i}") for i in range(3)))
    assert sorted(store.purge_queue) == ["user9"]
    purger.drain()
    assert store.purge_queue == {} and [len(c["files"]) for c in cloudflare.calls] == [30, 30, 30, 10]


def test_url_templates_override_the_default_list() -> None:
    config = purge.PurgeConfig.from_env({
        "PURGE_CLOUDFLARE_ZONE": ZONE, "PURGE_CLOUDFLARE_TOKEN": TOKEN, "PURGE_BASE_URL": BASE + "/",
        "PURGE_URL_TEMPLATES": "{base}/u/{name}/avatar, https://other.invalid/{name}/cover",
        "PURGE_TIME_BUDGET_S": "2.5"})
    assert config is not None and config.time_budget_s == 2.5
    assert config.urls(purge.PurgeItem("alice", 1, True, True)) == \
        [f"{BASE}/u/alice/avatar", "https://other.invalid/alice/cover"]
    assert config.urls(purge.PurgeItem("alice", 1, False, True)) == ["https://other.invalid/alice/cover"]
    with pytest.raises(ValueError, match="exactly one of"):
        purge_config(url_templates=("{base}/u/{name}/profile",))


def set_env(monkeypatch: pytest.MonkeyPatch, **env: str) -> None:
    for name in ("PURGE_CLOUDFLARE_ZONE", "PURGE_CLOUDFLARE_TOKEN", "PURGE_BASE_URL", "PURGE_URL_TEMPLATES",
                 "PURGE_TIME_BUDGET_S", "PURGE_CLOUDFLARE_API_URL"):
        monkeypatch.delenv(name, raising=False)
    env = {"PROXY_WHITELIST_SOURCE_URL": SOURCE_URL, "POSTGRES_URL": "postgresql://replica.invalid/proxy_whitelist",
           **env}
    for name, value in env.items():
        monkeypatch.setenv(name, value)


def test_purging_without_its_full_configuration_is_off_queues_nothing_and_is_logged_at_start(
        http: HttpFixture, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    store = FakeStore()
    monkeypatch.setattr(sync, "PgStore", lambda url: store)
    set_env(monkeypatch, PURGE_CLOUDFLARE_ZONE=ZONE, PURGE_CLOUDFLARE_TOKEN=TOKEN)
    with caplog.at_level(logging.INFO, logger="proxy_whitelist_replica"):
        assert sync.main(["--once"]) == 0
        http.source.head = 21
        http.source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
        assert sync.main(["--once"]) == 0

    assert store.bootstrap_complete and store.profiles["alice"][0] == "https://a.invalid/avatar2.png"
    assert store.purge_queue == {} and not store.purge_enabled
    assert sum("Cloudflare purging is off" in r.getMessage() for r in caplog.records) == 2, "once per start"
    assert TOKEN not in caplog.text


def test_purging_configured_from_the_environment_purges_and_never_logs_the_token(
        http: HttpFixture, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    store = FakeStore()
    syncer(http.source, store).cycle()
    cloudflare = Cloudflare(store)
    cloudflare.answers = [429]
    monkeypatch.setattr(sync.urllib.request, "urlopen", lambda req, timeout: (
        cloudflare if req.full_url.startswith(CF_API) else http).urlopen(req, timeout))
    monkeypatch.setattr(sync, "PgStore", lambda url: store)
    set_env(monkeypatch, PURGE_CLOUDFLARE_ZONE=ZONE, PURGE_CLOUDFLARE_TOKEN=TOKEN, PURGE_BASE_URL=BASE,
            PURGE_CLOUDFLARE_API_URL=CF_API)
    http.source.head = 21
    http.source.set_profile("alice", "https://a.invalid/avatar2.png", None, 21)
    with caplog.at_level(logging.DEBUG, logger="proxy_whitelist_replica"):
        assert sync.main(["--once"]) == 0
    assert store.purge_enabled and store.error is None
    assert len(cloudflare.calls) == 1 and "HTTP 429" in store.purge_queue["alice"]["last_error"]
    assert "retrying in 7.0 s" in caplog.text and "Cloudflare purging is on" in caplog.text

    with caplog.at_level(logging.DEBUG, logger="proxy_whitelist_replica"):
        assert sync.main(["--once"]) == 0
    assert len(cloudflare.calls) == 2 and cloudflare.files() == avatar_urls("alice") and store.purge_queue == {}
    assert TOKEN not in caplog.text
