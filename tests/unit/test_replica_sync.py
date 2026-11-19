"""replica/sync.py's paging, cursors and source identity handling against an
in-memory source that serves stream contract v2 and an in-memory store. No
database, no network."""
import hashlib
import importlib.util
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("replica_sync", ROOT / "replica" / "sync.py")
assert _spec is not None and _spec.loader is not None
sync = importlib.util.module_from_spec(_spec)
sys.modules["replica_sync"] = sync
_spec.loader.exec_module(sync)

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
        self.fail_on: str | None = None

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

    def load(self) -> Any:
        return sync.State(self.uuid, self.version, dict(self.cursors), self.bootstrap_complete)

    def adopt_source(self, source_url: str, status: Any) -> None:
        self.uuid, self.version = status.sync_uuid, status.stream_schema_version

    def reset(self, source_url: str, status: Any) -> None:
        resets = self.resets
        self._fresh()
        self.resets = resets + 1
        self.adopt_source(source_url, status)

    def apply_page(self, stream: Any, rows: list[dict[str, Any]], cursor: Any) -> None:
        if self.fail_on == stream.name:
            raise sync.SyncError("replica database: constraint violation")
        for r in rows:
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

    def replace_blacklists(self, urls: list[str], accounts: list[str]) -> None:
        self.blacklist = (urls, accounts)

    def finish_cycle(self, source_url: str, status: Any) -> None:
        self.source_block, self.source_time = status.block_num, status.block_time
        self.error = None
        self.bootstrap_complete = True

    def set_error(self, message: str) -> None:
        self.error = message


def syncer(source: FakeSource, store: FakeStore, page_size: int = 100, reset: bool = False) -> Any:
    return sync.Syncer(source, store, "http://primary.invalid/proxy-whitelist-api", page_size, reset)


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

    assert sync.run(s, 5, once=False, sleep=lambda _: None) == 2
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
