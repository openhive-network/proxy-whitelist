#!/usr/bin/env python3
"""Run replica/sync.py against the stack's primary and compare the replica's API with it.

    replica_smoke.py sync OUT_DIR    bootstrap the replica from the installed primary
    replica_smoke.py smoke OUT_DIR   seed the primary and check the replica follows it
    replica_smoke.py health OUT_DIR  rebuild the replica and run its haproxy agent check

Both serve the primary (PROXY_WHITELIST_TEST_USER_URL) and the replica
(PROXY_WHITELIST_TEST_REPLICA_USER_URL, installed by run-checks' replica-install
step) with PostgREST the way api_smoke.py does, each behind an in-process
rewriter that applies rewrite_rules.conf the way the postgrest-rewriter image
does, so the syncer reads the primary's real /stream/* paths. replica/sync.py
runs with --once as a subprocess (its logs go to OUT_DIR/replica-<mode>-sync-*.log).

smoke seeds the primary at fixed blocks far above the testnet's (moving the
app's processed block, like api_smoke.py), then checks, cycle by cycle: a
PAGE_LIMIT=2 cycle over 5 urls of one block, a url and an avatar holding a
backslash (and the url a double quote, a tab and a non-ASCII character) stored
exactly, byte-for-byte equal answers of
check_url, is_whitelisted, url_accounts, avatar and cover, the replica's
sync_status, 404 for /stream/*, new rows and empty cycles after bootstrap, a
url author delivered below its url's block, an unreachable source, a
changed source identity (refused, then --reset), a forced rebuild (--rebuild,
and PROXY_WHITELIST_REPLICA_REBUILD=1 honoured once) and Cloudflare purging
against an in-process stub of its purge_cache API (a changed avatar purged, a
503 leaving the account pending and purged by the next run). Writes
OUT_DIR/replica-<mode>.xml (one junit case per check); the seeded primary rows,
block and sync_uuid are restored at the end.

health rebuilds the replica from the (restored) primary, then runs
replica/healthcheck/proxy_whitelist_replica.sh against the replica's PostgREST
admin server and database: up when in sync, down (exit 10) when last_block_time
is older than 60 s, before the bootstrap completes and when PostgREST is not
live, and up with a purge backlog in its reason. The testnet primary produces no blocks, so the age is set in the
replica's state rather than waited for. Every case runs twice: with the suite's
psql and with a shim that, like the image's psql 14, prints only the last
statement's result of a multi-statement --command.
"""
import contextlib
import hashlib
import http.server
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Callable, Iterator

from api_smoke import call, call_raw, free_port, postgrest, psql, write_junit, PostgrestDidNotStart

ROOT = pathlib.Path(__file__).resolve().parents[1]
USER_URL = os.environ["PROXY_WHITELIST_TEST_USER_URL"]
REPLICA_ADMIN_URL = os.environ["PROXY_WHITELIST_TEST_REPLICA_ADMIN_URL"]
REPLICA_USER_URL = os.environ["PROXY_WHITELIST_TEST_REPLICA_USER_URL"]
SYNC_TIMEOUT_S = 300

PREFIX = f"https://aidev.invalid/replica/{int(time.time())}"
ALICE, ALICE_ID = "aidev-replica-a", 2147000101
BOB, BOB_ID = "aidev-replica-b", 2147000102
CAROL, CAROL_ID = "aidev-replica-c", 2147000103
UNKNOWN_ACCOUNT = "aidev-replica-none"
URL1, BLACK, NEW, LATE, DOWN, AFTER_UUID = (f"{PREFIX}-{n}.png" for n in ("1", "black", "new", "late", "down", "uuid"))
PAGE_URLS = [f"{PREFIX}-page-{i}.png" for i in range(5)]
AVATAR, AVATAR2, COVER = (f"{PREFIX}-{n}.png" for n in ("avatar\\_1", "avatar2", "cover"))
# PostgREST's text/csv doubles the backslash and escapes the quote and the tab.
ODD = f"{PREFIX}-IMG\\_7570s \"q\"\t\u00e9.jpg"
B = 910_000_000
SEED_BLOCK, PAGE_BLOCK, PROFILE_BLOCK, HEAD1 = B + 10, B + 15, B + 20, B + 30
NEW_BLOCK, QUIET_HEAD, LATE_AUTHOR_BLOCK, LATE_BLOCK, DOWN_BLOCK, UUID_BLOCK = B + 31, B + 40, B + 45, B + 50, B + 55, B + 60
PURGE_BLOCK, PURGE_FAIL_BLOCK = B + 65, B + 70
PURGE_ZONE, PURGE_TOKEN, PURGE_BASE = "aidev-zone", "aidev-purge-token", "https://images.aidev.invalid"
AVATAR3 = f"{PREFIX}-avatar3.png"

_restore: dict[str, Any] = {}


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def replica_sql(sql: str) -> str:
    return subprocess.run(["psql", REPLICA_ADMIN_URL, "-v", "ON_ERROR_STOP=on", "-Atc", sql],
                          check=True, capture_output=True, text=True).stdout.strip()


def _rewrite_rules() -> list[tuple[re.Pattern[str], str]]:
    rules = []
    for line in (ROOT / "rewrite_rules.conf").read_text().splitlines():
        m = re.match(r"\s*rewrite\s+(\S+)\s+(\S+)\s+break;", line)
        if m:
            rules.append((re.compile(m.group(1)), re.sub(r"\$(\d)", r"\\\1", m.group(2))))
    return rules


def rewrite(path_and_query: str, rules: list[tuple[re.Pattern[str], str]]) -> str:
    """nginx's `rewrite ... break` of rewrite_rules.conf: the first matching rule rewrites the
    path, and the request's arguments follow the replacement's own."""
    path, _, query = path_and_query.partition("?")
    for pattern, replacement in rules:
        match = pattern.search(path)
        if match:
            path = match.expand(replacement)
            break
    if query:
        path += ("&" if "?" in path else "?") + query
    return path


@contextlib.contextmanager
def rewriter(upstream: str) -> Iterator[str]:
    """An HTTP front of upstream (a PostgREST) applying rewrite_rules.conf; yields its base URL."""
    rules = _rewrite_rules()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _forward(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            headers = {k: v for k, v in self.headers.items() if k.lower() in ("accept", "content-type")}
            req = urllib.request.Request(upstream + rewrite(self.path, rules), data=body,
                                         method=self.command, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=SYNC_TIMEOUT_S) as resp:
                    status, resp_headers, payload = resp.status, resp.headers, resp.read()
            except urllib.error.HTTPError as err:
                status, resp_headers, payload = err.code, err.headers, err.read()
            self.send_response(status)
            for name, value in resp_headers.items():
                if name.lower() not in ("transfer-encoding", "connection", "content-length", "date", "server"):
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = _forward

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", free_port()), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class Run:
    """The two APIs and the syncer of one mode's run."""

    def __init__(self, out_dir: str, mode: str, primary: str, replica: str, admin: str | None = None) -> None:
        self.out_dir = out_dir
        self.mode = mode
        self.primary = primary
        self.replica = replica
        self.admin = admin
        self.syncs = 0

    def sync(self, *args: str, source: str | None = None, page_limit: int = 100000, rebuild_env: str = "0",
             purge_api: str | None = None) -> int:
        """One replica/sync.py --once; purge_api enables purging against that Cloudflare API base."""
        self.syncs += 1
        env = dict(os.environ, PROXY_WHITELIST_SOURCE_URL=source or self.primary, POSTGRES_URL=REPLICA_ADMIN_URL,
                   PAGE_LIMIT=str(page_limit), PROXY_WHITELIST_REPLICA_RESET="0",
                   PROXY_WHITELIST_REPLICA_REBUILD=rebuild_env, LOG_LEVEL="DEBUG")
        for name in ("PURGE_CLOUDFLARE_ZONE", "PURGE_CLOUDFLARE_TOKEN", "PURGE_BASE_URL", "PURGE_URL_TEMPLATES"):
            env.pop(name, None)
        if purge_api:
            env.update(PURGE_CLOUDFLARE_ZONE=PURGE_ZONE, PURGE_CLOUDFLARE_TOKEN=PURGE_TOKEN, PURGE_BASE_URL=PURGE_BASE,
                       PURGE_CLOUDFLARE_API_URL=purge_api)
        log_path = os.path.join(self.out_dir, self.log_name())
        with open(log_path, "w", encoding="utf-8") as log:
            return subprocess.run([sys.executable, str(ROOT / "replica" / "sync.py"), "--once", *args],
                                  env=env, stdout=log, stderr=subprocess.STDOUT, timeout=SYNC_TIMEOUT_S).returncode

    def synced(self, *args: str, **kwargs: Any) -> None:
        rc = self.sync(*args, **kwargs)
        assert rc == 0, f"replica/sync.py exited {rc}; see {self.log_name()}"

    def log_name(self) -> str:
        return f"replica-{self.mode}-sync-{self.syncs}.log"

    def status(self) -> dict[str, Any]:
        status, body = call(self.replica, "GET", "/sync-status")
        assert status == 200 and isinstance(body, dict), f"replica /sync-status: HTTP {status} {body!r}"
        return body

    def same(self, method: str, path: str, body: Any = None, want_status: int = 200) -> bytes:
        """Asserts the primary and the replica answer byte-for-byte alike; returns the answer."""
        p_status, p_body = call_raw(self.primary, method, path, body)
        r_status, r_body = call_raw(self.replica, method, path, body)
        assert p_status == want_status, f"primary {method} {path}: HTTP {p_status} {p_body!r}"
        assert (r_status, r_body) == (p_status, p_body), \
            f"{method} {path}: primary {p_status} {p_body!r}, replica {r_status} {r_body!r}"
        return r_body


def primary_head() -> int:
    return int(psql("select current_block_num from hafd.contexts where name = 'proxy_whitelist_app'"))


def primary_sync_uuid() -> str:
    return psql("select sync_uuid from proxy_whitelist_app.app_status")


def set_head(block: int) -> None:
    psql(f"set role proxy_whitelist_owner; select hive.app_set_current_block_num('proxy_whitelist_app', {block});")


def whitelist(urls: list[str], account: str, hive_id: int, block: int) -> None:
    listed = ", ".join(f"'{u}'" for u in urls)
    psql(f"""
        set role proxy_whitelist_owner;
        select proxy_whitelist_app.whitelist_urls(
            array[{listed}], proxy_whitelist_app.ensure_account({hive_id}, '{account}', {block}), {block});
    """)


def set_profile(account: str, hive_id: int, avatar: str, cover: str, block: int) -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        select proxy_whitelist_app.set_account_profile({hive_id}, '{account}', '{avatar}', '{cover}', {block});
    """)


def counts(run_sql: Callable[[str], str], head: int) -> str:
    """The row counts a replica at `head` must match; accounts the install's
    backfill labelled above the primary's head are not served yet."""
    return run_sql(f"""
        select format('accounts %s, urls %s, url_authors %s, profiles %s, blacklisted urls %s, blacklisted accounts %s',
            (select count(*) from proxy_whitelist_app.accounts where first_block <= {head}),
            (select count(*) from proxy_whitelist_app.urls),
            (select count(*) from proxy_whitelist_app.url_authors),
            (select count(*) from proxy_whitelist_app.account_profiles),
            (select count(*) from proxy_whitelist_app.blacklisted_urls),
            (select count(*) from proxy_whitelist_app.blacklisted_accounts))""")


def same_counts() -> None:
    head = primary_head()
    primary, replica = counts(psql, head), counts(replica_sql, head)
    assert primary == replica, f"primary has {primary}, replica {replica}"


def same_answers(run: Run) -> None:
    for url in (URL1, BLACK, PAGE_URLS[0], f"{PREFIX}-unseen.png"):
        run.same("POST", "/whitelist/check", {"url": url})
        run.same("POST", "/whitelist/is-whitelisted", {"url": url})
        run.same("POST", "/whitelist/url-accounts", {"url": url})
    run.same("POST", "/whitelist/check", {"url": URL1, "blacklisted_accounts": [ALICE]})
    run.same("POST", "/whitelist/check", {"url": URL1, "blacklisted_accounts": ["someone-else"]})
    run.same("POST", "/whitelist/url-blacklisted", {"url": BLACK})
    for account in (ALICE, BOB):
        run.same("GET", f"/avatar/{account}")
        run.same("GET", f"/cover/{account}")
    run.same("GET", f"/avatar/{UNKNOWN_ACCOUNT}", want_status=404)


def seed() -> None:
    _restore["head"] = primary_head()
    _restore["sync_uuid"] = primary_sync_uuid()
    whitelist([URL1, BLACK, ODD], ALICE, ALICE_ID, SEED_BLOCK)
    whitelist(PAGE_URLS, ALICE, ALICE_ID, PAGE_BLOCK)
    psql(f"""
        set role proxy_whitelist_owner;
        select proxy_whitelist_app.ensure_account({BOB_ID}, '{BOB}', {PAGE_BLOCK});
        insert into proxy_whitelist_app.blacklisted_urls (url_id)
            select id from proxy_whitelist_app.urls where url = '{BLACK}';
    """)
    set_profile(ALICE, ALICE_ID, AVATAR, COVER, PROFILE_BLOCK)
    set_head(HEAD1)


def cleanup() -> None:
    urls = f"(select id from proxy_whitelist_app.urls where url like '{PREFIX}%')"
    accounts = f"(select id from proxy_whitelist_app.accounts where name in ('{ALICE}', '{BOB}', '{CAROL}'))"
    restore = ""
    if "head" in _restore:
        restore = f"""
            select hive.app_set_current_block_num('proxy_whitelist_app', {_restore['head']});
            update proxy_whitelist_app.app_status set sync_uuid = '{_restore['sync_uuid']}';"""
    psql(f"""
        set role proxy_whitelist_owner;
        delete from proxy_whitelist_app.blacklisted_urls where url_id in {urls};
        delete from proxy_whitelist_app.url_authors where url_id in {urls};
        delete from proxy_whitelist_app.urls where id in {urls};
        delete from proxy_whitelist_app.account_profiles where account_id in {accounts};
        delete from proxy_whitelist_app.accounts where id in {accounts};
        {restore}
    """)


def check_bootstrap(run: Run) -> None:
    run.synced()
    status = run.status()
    assert status["bootstrap_complete"] is True, f"bootstrap_complete in {status}"
    assert status["sync_error"] is None, f"sync_error in {status}"
    assert status["last_block_num"] == primary_head(), f"last_block_num {status['last_block_num']}, primary {primary_head()}"
    assert status["source_sync_uuid"] == primary_sync_uuid(), f"source_sync_uuid in {status}"
    same_counts()


def check_paged_cycle(run: Run) -> None:
    seed()
    run.synced(page_limit=2)
    got = replica_sql(f"select count(*) from proxy_whitelist_app.urls where url like '{PREFIX}-page-%'")
    assert got == "5", f"{got} page urls on the replica"
    same_counts()


def check_exact_values(run: Run) -> None:
    got = replica_sql(f"select md5(url) from proxy_whitelist_app.urls where url = '{ODD}'")
    assert got == md5(ODD), f"replica md5 of {ODD!r}: {got!r}"
    assert run.same("POST", "/whitelist/check", {"url": ODD}) == b'"whitelisted"'
    assert json.loads(run.same("GET", f"/avatar/{ALICE}")) == AVATAR


def check_sync_status_shape(run: Run) -> None:
    _, primary = call(run.primary, "GET", "/sync-status")
    status = run.status()
    missing = set(primary) - set(status)
    assert not missing, f"replica /sync-status lacks the primary's {missing}"
    extra = {"source_sync_uuid", "source_url", "polled_at", "bootstrap_complete", "sync_error",
             "purge_enabled", "purge_pending", "purge_oldest_block", "purge_last_error"}
    assert extra <= set(status), f"replica /sync-status lacks {extra - set(status)}"
    assert status["last_block_num"] == HEAD1 and status["source_sync_uuid"] == primary["sync_uuid"], f"{status} vs {primary}"
    assert status["sync_uuid"] == replica_sql("select replica_sync_uuid from proxy_whitelist_replica.state") \
        and status["sync_uuid"] != primary["sync_uuid"], f"the replica's sync_uuid in {status}, the primary's {primary['sync_uuid']}"
    assert status["source_url"] == run.primary and status["polled_at"], f"{status}"


def check_no_streams(run: Run) -> None:
    for path in ("/stream/urls?from_block=0&page_size=1", "/stream/blacklists"):
        status, body = call(run.replica, "GET", path)
        assert status == 404, f"replica GET {path}: HTTP {status} {body!r}"


def check_new_rows(run: Run) -> None:
    whitelist([NEW], CAROL, CAROL_ID, NEW_BLOCK)
    set_profile(ALICE, ALICE_ID, AVATAR2, COVER, NEW_BLOCK)
    set_head(NEW_BLOCK)
    run.synced()
    assert run.same("POST", "/whitelist/check", {"url": NEW}) == b'"whitelisted"'
    assert json.loads(run.same("GET", f"/avatar/{ALICE}")) == AVATAR2
    assert run.status()["last_block_num"] >= NEW_BLOCK


def check_quiet_cycles(run: Run) -> None:
    for head in (QUIET_HEAD - 5, QUIET_HEAD):
        set_head(head)
        run.synced()
        assert run.status()["last_block_num"] == head, f"{run.status()} after head {head}"
    cursors = replica_sql("select accounts_block, urls_block, url_authors_block, profiles_block from proxy_whitelist_replica.state")
    assert cursors == "|".join([str(QUIET_HEAD)] * 4), f"cursors {cursors}"


def check_pending_author(run: Run) -> None:
    whitelist([LATE], BOB, BOB_ID, LATE_BLOCK)
    psql(f"""
        set role proxy_whitelist_owner;
        update proxy_whitelist_app.url_authors set first_block = {LATE_AUTHOR_BLOCK}
        where url_id = (select id from proxy_whitelist_app.urls where url = '{LATE}');
    """)
    set_head(LATE_AUTHOR_BLOCK)
    run.synced()
    pending = replica_sql("select url_md5 || ' ' || account_name from proxy_whitelist_replica.pending_url_authors")
    assert pending == f"{md5(LATE)} {BOB}", f"pending_url_authors: {pending!r}"
    set_head(LATE_BLOCK)
    run.synced()
    assert replica_sql("select count(*) from proxy_whitelist_replica.pending_url_authors") == "0"
    assert json.loads(run.same("POST", "/whitelist/url-accounts", {"url": LATE})) == [{"account": BOB}]


def check_unreachable_source(run: Run) -> None:
    before = run.status()
    whitelist([DOWN], CAROL, CAROL_ID, DOWN_BLOCK)
    set_head(DOWN_BLOCK)
    rc = run.sync(source=f"http://127.0.0.1:{free_port()}")
    assert rc == 1, f"replica/sync.py exited {rc} with the source down"
    down = run.status()
    assert down["sync_error"], f"no sync_error with the source down: {down}"
    assert (down["last_block_num"], down["last_block_time"]) == (before["last_block_num"], before["last_block_time"]), \
        f"{down} advanced from {before}"
    run.synced()
    assert run.status()["sync_error"] is None and run.status()["last_block_num"] == DOWN_BLOCK
    same_counts()


def check_identity_change(run: Run) -> None:
    psql(f"update proxy_whitelist_app.app_status set sync_uuid = '{uuid.uuid4()}'")
    whitelist([AFTER_UUID], CAROL, CAROL_ID, UUID_BLOCK)
    set_head(UUID_BLOCK)
    rc = run.sync()
    assert rc == 2, f"replica/sync.py exited {rc} on a changed sync_uuid"
    status = run.status()
    assert status["sync_error"] and "sync_uuid" in status["sync_error"], f"{status}"
    assert status["last_block_num"] == DOWN_BLOCK, f"{status}"
    assert call(run.replica, "POST", "/whitelist/check", {"url": AFTER_UUID})[1] == "unknown"
    assert call(run.replica, "POST", "/whitelist/check", {"url": URL1})[1] == "whitelisted"
    run.synced("--reset")
    status = run.status()
    assert status["source_sync_uuid"] == primary_sync_uuid() and status["sync_error"] is None, f"{status}"
    same_counts()
    same_answers(run)


def check_rebuild(run: Run) -> None:
    # cli/blacklist.py remove-all style: a deletion on the primary that the streams do not carry
    psql(f"""
        set role proxy_whitelist_owner;
        delete from proxy_whitelist_app.url_authors
            where url_id = (select id from proxy_whitelist_app.urls where url = '{PAGE_URLS[4]}');
        delete from proxy_whitelist_app.urls where url = '{PAGE_URLS[4]}';
    """)
    run.synced()
    assert replica_sql(f"select count(*) from proxy_whitelist_app.urls where url = '{PAGE_URLS[4]}'") == "1"
    before = run.status()["sync_uuid"]
    run.synced("--rebuild")
    status = run.status()
    assert status["bootstrap_complete"] is True and status["sync_error"] is None, f"{status}"
    assert status["sync_uuid"] != before, f"--rebuild kept the replica's sync_uuid {before}"
    same_counts()
    same_answers(run)

    run.synced(rebuild_env="1")
    rebuilt = run.status()["sync_uuid"]
    assert rebuilt != status["sync_uuid"], "PROXY_WHITELIST_REPLICA_REBUILD=1 did not rebuild"
    run.synced(rebuild_env="1")
    assert run.status()["sync_uuid"] == rebuilt, "PROXY_WHITELIST_REPLICA_REBUILD=1 rebuilt again on a second start"
    same_counts()


class CloudflareStub:
    """Cloudflare's POST /zones/{zone}/purge_cache: records each call's files, answers `status`."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.status = 200
        self.calls: list[list[str]] = []


@contextlib.contextmanager
def cloudflare_stub() -> Iterator[CloudflareStub]:
    stub: CloudflareStub

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
            authorized = (self.path == f"/client/v4/zones/{PURGE_ZONE}/purge_cache"
                          and self.headers.get("Authorization") == f"Bearer {PURGE_TOKEN}")
            stub.calls.append(sorted(body["files"]) if authorized else [f"unauthorized {self.path}"])
            status = stub.status if authorized else 403
            payload = json.dumps({"success": status == 200, "errors": [], "messages": []}).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", free_port()), Handler)
    stub = CloudflareStub(f"http://127.0.0.1:{server.server_address[1]}/client/v4")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield stub
    finally:
        server.shutdown()
        server.server_close()


def purge_urls(account: str, *kinds: str) -> list[str]:
    paths = {"avatar": ["avatar", "avatar/small", "avatar/medium", "avatar/large"], "cover": ["cover"]}
    return sorted(f"{PURGE_BASE}{prefix}/u/{account}/{p}" for kind in kinds for p in paths[kind] for prefix in ("", "/webp"))


def check_purge(run: Run) -> None:
    with cloudflare_stub() as stub:
        set_profile(ALICE, ALICE_ID, AVATAR3, COVER, PURGE_BLOCK)
        set_head(PURGE_BLOCK)
        run.synced(purge_api=stub.url)
        assert stub.calls == [purge_urls(ALICE, "avatar")], f"purge calls {stub.calls}"
        status = run.status()
        assert (status["purge_enabled"], status["purge_pending"], status["purge_oldest_block"]) == (True, 0, None), \
            f"{status}"
        assert json.loads(run.same("GET", f"/avatar/{ALICE}")) == AVATAR3

        stub.status = 503
        set_profile(BOB, BOB_ID, AVATAR, COVER, PURGE_FAIL_BLOCK)
        set_head(PURGE_FAIL_BLOCK)
        run.synced(purge_api=stub.url)
        status = run.status()
        assert (status["purge_pending"], status["purge_oldest_block"]) == (1, PURGE_FAIL_BLOCK), f"{status}"
        assert "HTTP 503" in (status["purge_last_error"] or ""), f"{status}"
        assert status["sync_error"] is None and status["last_block_num"] == PURGE_FAIL_BLOCK, f"{status}"
        assert len(stub.calls) == 2, f"purge calls {stub.calls}"

        stub.status = 200
        run.synced(purge_api=stub.url)
        assert stub.calls[2:] == [purge_urls(BOB, "avatar", "cover")], f"purge calls {stub.calls}"
        status = run.status()
        assert (status["purge_pending"], status["purge_last_error"]) == (0, None), f"{status}"
    run.synced()
    assert run.status()["purge_enabled"] is False


SMOKE_CHECKS: list[tuple[str, Callable[[Run], None]]] = [
    ("a PAGE_LIMIT=2 cycle replicates exactly the 5 urls of one block", check_paged_cycle),
    ("a url and an avatar with a backslash, a quote, a tab and é are replicated exactly", check_exact_values),
    ("check_url, is_whitelisted, url_accounts, avatar and cover answer like the primary", same_answers),
    ("sync_status has the primary's fields and the replica's", check_sync_status_shape),
    ("the replica serves no /stream/*", check_no_streams),
    ("rows added after bootstrap arrive in the next cycle", check_new_rows),
    ("cycles without new rows advance to the source's block", check_quiet_cycles),
    ("a url author below its url's block waits in pending_url_authors", check_pending_author),
    ("an unreachable source sets sync_error and freezes last_block_time; syncing resumes", check_unreachable_source),
    ("a changed sync_uuid is refused, --reset bootstraps again to equality", check_identity_change),
    ("--rebuild and PROXY_WHITELIST_REPLICA_REBUILD=1 (once) bootstrap again to equality", check_rebuild),
    ("a changed avatar is purged at Cloudflare; a 503 leaves it pending until the next run", check_purge),
]
AGENT = ROOT / "replica" / "healthcheck" / "proxy_whitelist_replica.sh"
# busybox date's -d "YYYY.MM.DD-HH:MM:SS" (check_http_sync_age.sh) for the suites' coreutils date.
BUSYBOX_DATE = """#!/bin/bash
args=()
for a in "$@"; do
  if [[ $a =~ ^([0-9]{4})\\.([0-9]{2})\\.([0-9]{2})-(.*)$ ]]; then
    a="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]} ${BASH_REMATCH[4]}"
  fi
  args+=("$a")
done
exec %s "${args[@]}"
"""
# psql before 15 (the proxy-whitelist image's is 14): of a --command holding several
# statements it prints only the last statement's result. Emulated with the suite's
# psql in one session: every statement but the last runs with output to /dev/null.
# Statements are split at every ';' (the agent's queries hold no quoted ';').
PSQL_BEFORE_15 = """#!/bin/bash
args=()
for a in "$@"; do
  case $a in
    --command=*|-c*)
      [[ $a == --command=* ]] && sql=${a#--command=} || sql=${a#-c}
      IFS=';' read -ra parts <<< "$sql"
      stmts=()
      for p in "${parts[@]}"; do [[ $p =~ [^[:space:]] ]] && stmts+=("$p"); done
      if (( ${#stmts[@]} > 1 )); then
        head=$(IFS=';'; echo "${stmts[*]:0:${#stmts[@]}-1}")
        args+=(-c '\\o /dev/null' -c "$head" -c '\\o' -c "${stmts[-1]}")
      else
        args+=("$a")
      fi ;;
    *) args+=("$a") ;;
  esac
done
exec %s "${args[@]}"
"""


def agent(live_url: str, psql_before_15: bool) -> tuple[int, str]:
    """One run of the agent check: its exit status and its answer. psql_before_15 runs it
    with a psql printing only the last statement's result, like the image's psql 14."""
    with tempfile.TemporaryDirectory() as bin_dir:
        shims = {"date": BUSYBOX_DATE % shutil.which("date")}
        if psql_before_15:
            shims["psql"] = PSQL_BEFORE_15 % shutil.which("psql")
        for name, text in shims.items():
            shim = pathlib.Path(bin_dir) / name
            shim.write_text(text)
            shim.chmod(0o755)
        env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}",
                   REPLICA_LIVE_URL=live_url, REPLICA_POSTGRES_URL=REPLICA_USER_URL)
        done = subprocess.run(["sh", str(AGENT)], env=env, capture_output=True, text=True, timeout=30)
    return done.returncode, done.stdout.strip() + done.stderr.strip()


def check_health(run: Run) -> None:
    run.synced("--rebuild")
    same_counts()
    assert run.admin, "no PostgREST admin server"
    live = f"{run.admin}/live"
    state = replica_sql("select source_block_num, source_block_time from proxy_whitelist_replica.state")
    try:
        for before_15 in (True, False):
            psql_ = "psql < 15" if before_15 else "psql >= 15"
            replica_sql("update proxy_whitelist_replica.state set bootstrap_complete = true, "
                        f"source_block_num = {state.partition('|')[0] or 1}, source_block_time = now()")
            assert agent(live, before_15) == (0, "up"), f"in sync, {psql_}: {agent(live, before_15)}"
            replica_sql("insert into proxy_whitelist_replica.purge_queue (account_name, block, avatar_changed, "
                        "cover_changed) values ('aidev-purge-backlog', 123, true, false)")
            try:
                assert agent(live, before_15) == (0, "up #proxy_whitelist_replica purge backlog: 1 accounts since "
                                                     "block 123"), f"purge backlog, {psql_}: {agent(live, before_15)}"
            finally:
                replica_sql("delete from proxy_whitelist_replica.purge_queue where account_name = 'aidev-purge-backlog'")
            replica_sql("update proxy_whitelist_replica.state set source_block_time = now() - interval '2 minutes'")
            rc, answer = agent(live, before_15)
            assert (rc, answer.split(" (")[0]) == (10, "down #proxy_whitelist_replica block too old"), \
                f"stale, {psql_}: {rc} {answer!r}"
            assert agent(f"http://127.0.0.1:{free_port()}/live", before_15) == \
                (10, "down #proxy_whitelist_replica postgrest not live"), psql_
            replica_sql("update proxy_whitelist_replica.state set source_block_num = null, source_block_time = null, "
                        "bootstrap_complete = false")
            assert agent(live, before_15) == (10, "down #proxy_whitelist_replica has no processed block yet"), \
                f"{psql_}: {agent(live, before_15)}"
    finally:
        block, _, time_ = state.partition("|")
        replica_sql(f"update proxy_whitelist_replica.state set bootstrap_complete = true, "
                    f"source_block_num = {block or 'null'}, source_block_time = {repr(time_) if time_ else 'null'}")
    code = [line for p in AGENT.parent.glob("*.sh") for line in p.read_text().splitlines()
            if not line.lstrip().startswith("#")]
    haf = [line for line in code if re.search(r"\b(hive|hafd)\.", line)]
    assert not haf, f"the replica agent queries HAF: {haf}"


MODES: dict[str, list[tuple[str, Callable[[Run], None]]]] = {
    "sync": [("a fresh replica bootstraps from the primary", check_bootstrap)],
    "smoke": SMOKE_CHECKS,
    "health": [("the haproxy agent check answers up in sync, down when stale, unbootstrapped or not live",
                check_health)],
}


def main(mode: str, out_dir: str) -> int:
    results = []
    with contextlib.ExitStack() as stack:
        stack.callback(cleanup)
        try:
            primary = stack.enter_context(rewriter(stack.enter_context(
                postgrest(USER_URL, os.path.join(out_dir, f"replica-{mode}-primary-postgrest.log")))))
            admin_port = free_port()
            replica = stack.enter_context(rewriter(stack.enter_context(
                postgrest(REPLICA_USER_URL, os.path.join(out_dir, f"replica-{mode}-postgrest.log"), admin_port))))
        except PostgrestDidNotStart as exc:
            print(exc, file=sys.stderr)
            results.append(("postgrest start", 0.0, str(exc)))
            write_junit(os.path.join(out_dir, f"replica-{mode}.xml"), results)
            return 1
        run = Run(out_dir, mode, primary, replica, f"http://127.0.0.1:{admin_port}")
        for name, check in MODES[mode]:
            t0 = time.time()
            error = ""
            try:
                check(run)
                print(f"ok   {name}")
            except (AssertionError, subprocess.SubprocessError, OSError, ValueError, KeyError) as exc:
                error = f"{type(exc).__name__}: {exc}"
                print(f"FAIL {name}: {error}", file=sys.stderr)
            results.append((name, time.time() - t0, error))
            if error:
                break
    write_junit(os.path.join(out_dir, f"replica-{mode}.xml"), results)
    return 1 if any(r[2] for r in results) else 0


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in MODES:
        sys.exit(f"usage: {sys.argv[0]} {'|'.join(MODES)} OUT_DIR")
    sys.exit(main(sys.argv[1], sys.argv[2]))
