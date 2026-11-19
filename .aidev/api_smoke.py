#!/usr/bin/env python3
"""Serve the installed proxy_whitelist schema with PostgREST and check its API answers.

    api_smoke.py OUT_DIR

Starts `postgrest` the way haf_api_node's proxy-whitelist-postgrest service does
(schema proxy_whitelist_endpoints, anonymous role proxy_whitelist_user, root spec
`root`, extra search path proxy_whitelist_app) on a free loopback port against
PROXY_WHITELIST_TEST_USER_URL, seeds one URL through psql on
PROXY_WHITELIST_TEST_ADMIN_URL, and makes the calls of CHECKS in order:

  root spec, sync_status, check_url unknown → (seeded) whitelisted →
  (seeded author blacklisted by the caller) unknown → (url blacklisted) blacklisted,
  url_accounts of the seeded URL, then the /stream/* pages (seeded at fixed
  blocks, with the app's processed block moved past them), a profile change,
  and avatar/cover.

Writes OUT_DIR/api-smoke.xml (one junit case per call) and exits non-zero when any
call fails. The seeded rows are removed and the app's block restored at the end.
"""
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Callable
from xml.sax.saxutils import escape, quoteattr

STARTUP_TIMEOUT_S = 60
ADMIN_URL = os.environ["PROXY_WHITELIST_TEST_ADMIN_URL"]
USER_URL = os.environ["PROXY_WHITELIST_TEST_USER_URL"]
SMOKE_PREFIX = f"https://aidev.invalid/smoke/{int(time.time())}"
SMOKE_URL = f"{SMOKE_PREFIX}.png"
SMOKE_ACCOUNT = "aidev-smoke"
SMOKE_HIVE_ID = 2147000001
PLAIN_ACCOUNT = "aidev-smoke-np"
PLAIN_HIVE_ID = 2147000002
UNKNOWN_ACCOUNT = "aidev-smoke-none"
PAGE_URLS = [f"{SMOKE_PREFIX}-page-{i}.png" for i in range(5)]
AVATAR, AVATAR2, COVER = (f"{SMOKE_PREFIX}-{n}.png" for n in ("avatar", "avatar2", "cover"))
# Fixed blocks far above a testnet's: the streams serve only blocks the app has
# processed, so seeding moves the app's block to STREAM_HEAD (restored by cleanup).
URL_BLOCK, PAGE_BLOCK, PROFILE_BLOCK, CHANGE_BLOCK, STREAM_HEAD = (
    900_000_010, 900_000_015, 900_000_020, 900_000_030, 900_000_100)
_original_head = None
_last_headers: dict[str, str] = {}


def psql(sql: str) -> str:
    return subprocess.run(
        ["psql", ADMIN_URL, "-v", "ON_ERROR_STOP=on", "-Atc", sql],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base: str, method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _last_headers.clear()
            _last_headers.update(resp.headers.items())
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, raw


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def seed_url() -> None:
    global _original_head
    _original_head = int(psql("select current_block_num from hafd.contexts where name = 'proxy_whitelist_app'"))
    psql(f"""
        set role proxy_whitelist_owner;
        select hive.app_set_current_block_num('proxy_whitelist_app', {STREAM_HEAD});
        select proxy_whitelist_app.whitelist_urls(
            array['{SMOKE_URL}'],
            proxy_whitelist_app.ensure_account({SMOKE_HIVE_ID}, '{SMOKE_ACCOUNT}'),
            {URL_BLOCK});
    """)


def seed_streams() -> None:
    page_urls = ", ".join(f"'{u}'" for u in PAGE_URLS)
    metadata = json.dumps({"profile": {"profile_image": AVATAR, "cover_image": COVER}})
    psql(f"""
        set role proxy_whitelist_owner;
        select proxy_whitelist_app.whitelist_urls(
            array[{page_urls}], proxy_whitelist_app.ensure_account({SMOKE_HIVE_ID}, '{SMOKE_ACCOUNT}'), {PAGE_BLOCK});
        select proxy_whitelist_app.set_account_profile(
            {SMOKE_HIVE_ID}, '{SMOKE_ACCOUNT}', img.avatar_url, img.cover_url, {PROFILE_BLOCK})
        from proxy_whitelist_app.extract_profile_images(NULL, '{metadata}') img;
        select proxy_whitelist_app.ensure_account({PLAIN_HIVE_ID}, '{PLAIN_ACCOUNT}');
    """)


def change_avatar() -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        select proxy_whitelist_app.set_account_profile(
            {SMOKE_HIVE_ID}, '{SMOKE_ACCOUNT}', '{AVATAR2}', '{COVER}', {CHANGE_BLOCK});
    """)


def blacklist_url() -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        insert into proxy_whitelist_app.blacklisted_urls (url_id)
            select id from proxy_whitelist_app.urls where url = '{SMOKE_URL}';
    """)


def cleanup() -> None:
    smoke_urls = f"(select id from proxy_whitelist_app.urls where url like '{SMOKE_PREFIX}%')"
    smoke_accounts = f"(select id from proxy_whitelist_app.accounts where name in ('{SMOKE_ACCOUNT}', '{PLAIN_ACCOUNT}'))"
    restore = (f"select hive.app_set_current_block_num('proxy_whitelist_app', {_original_head});"
               if _original_head is not None else "")
    psql(f"""
        set role proxy_whitelist_owner;
        delete from proxy_whitelist_app.blacklisted_urls where url_id in {smoke_urls};
        delete from proxy_whitelist_app.url_authors where url_id in {smoke_urls};
        delete from proxy_whitelist_app.urls where id in {smoke_urls};
        delete from proxy_whitelist_app.account_profiles where account_id in {smoke_accounts};
        delete from proxy_whitelist_app.accounts where id in {smoke_accounts};
        {restore}
    """)


def expect(status: int, body: Any, want_status: int, want_body: Any = None,
           want_type: type | tuple[type, ...] | None = None) -> None:
    assert status == want_status, f"HTTP {status}: {body!r}"
    if want_body is not None:
        assert body == want_body, f"expected {want_body!r}, got {body!r}"
    if want_type is not None:
        assert isinstance(body, want_type), f"expected {want_type}, got {body!r}"


def contains(body: Any, row: Any) -> None:
    assert isinstance(body, list), f"expected a list of rows, got {body!r}"
    assert row in body, f"{row!r} not in {body!r}"


def expect_contains(row: Any) -> Callable[[int, Any], None]:
    def check(status: int, body: Any) -> None:
        expect(status, body, 200)
        contains(body, row)
    return check


def expect_lists_smoke_author(status: int, body: Any) -> None:
    expect(status, body, 200)
    assert SMOKE_ACCOUNT in json.dumps(body), f"{SMOKE_ACCOUNT} not in {body!r}"


def expect_not_blacklisted(status: int, body: Any) -> None:
    expect(status, body, 200, want_type=dict)
    assert SMOKE_URL not in body["urls"], f"{SMOKE_URL} in {body!r}"


def expect_blacklisted(status: int, body: Any) -> None:
    expect(status, body, 200, want_type=dict)
    contains(body["urls"], SMOKE_URL)


def expect_smoke_account(status: int, body: Any) -> None:
    expect(status, body, 200)
    contains([{"hive_id": r["hive_id"], "name": r["name"]} for r in body],
             {"hive_id": SMOKE_HIVE_ID, "name": SMOKE_ACCOUNT})


def page_urls(base: str) -> tuple[int, Any]:
    """Read PAGE_BLOCK of /stream/urls two rows at a time, resuming after each page's last row."""
    rows, from_block, after = [], PAGE_BLOCK, ""
    while True:
        status, page = call(base, "GET", f"/rpc/stream_urls?from_block={from_block}&to_block={PAGE_BLOCK}&limit=2{after}")
        if status != 200:
            return status, page
        assert len(page) <= 2, f"page of {len(page)} rows for limit=2"
        rows += page
        if len(page) < 2:
            return status, rows
        from_block, after = page[-1]["first_block"], f"&after_md5={page[-1]['md5']}"


def expect_identity(status: int, body: Any) -> None:
    expect(status, body, 200, want_type=dict)
    uuid.UUID(body["sync_uuid"])
    assert body["stream_schema_version"] == 1, f"stream_schema_version {body['stream_schema_version']!r}"
    assert isinstance(body["stream_sentinel_block"], int), f"stream_sentinel_block {body['stream_sentinel_block']!r}"


def expect_url(want: str | None) -> Callable[[int, Any], None]:
    def check(status: int, body: Any) -> None:
        expect(status, body, 200)
        assert body == want, f"expected {want!r}, got {body!r}"
        assert _last_headers.get("Cache-Control") == "public, max-age=60", f"Cache-Control {_last_headers.get('Cache-Control')!r}"
    return check


# (name, setup or None, method or page reader, path, body, check)
CHECKS: list[tuple[str, Callable[[], None] | None, str | Callable[[str], tuple[int, Any]], str, Any,
                   Callable[[int, Any], object]]] = [
    ("root spec", None, "GET", "/", None,
     lambda s, b: expect(s, b, 200, want_type=dict)),
    ("sync_status", None, "GET", "/rpc/sync_status", None,
     lambda s, b: expect(s, b, 200, want_type=(dict, list))),
    ("check_url unknown before seeding", None, "POST", "/rpc/check_url", {"url": SMOKE_URL},
     lambda s, b: expect(s, b, 200, "unknown")),
    ("check_url whitelisted after seeding", seed_url, "POST", "/rpc/check_url", {"url": SMOKE_URL},
     lambda s, b: expect(s, b, 200, "whitelisted")),
    ("check_url unknown when the caller blacklists its only author", None, "POST", "/rpc/check_url",
     {"url": SMOKE_URL, "blacklisted_accounts": [SMOKE_ACCOUNT]},
     lambda s, b: expect(s, b, 200, "unknown")),
    ("check_url whitelisted when the caller blacklists another account", None, "POST", "/rpc/check_url",
     {"url": SMOKE_URL, "blacklisted_accounts": ["someone-else"]},
     lambda s, b: expect(s, b, 200, "whitelisted")),
    ("url_accounts lists the seeded author", None, "POST", "/rpc/url_accounts", {"url": SMOKE_URL},
     expect_lists_smoke_author),
    ("stream_blacklists does not list the url before blacklisting", None, "GET", "/rpc/stream_blacklists", None,
     expect_not_blacklisted),
    ("check_url blacklisted after blacklisting the url", blacklist_url, "POST", "/rpc/check_url", {"url": SMOKE_URL},
     lambda s, b: expect(s, b, 200, "blacklisted")),
    ("is_whitelisted of an unseen url is false", None, "POST", "/rpc/is_whitelisted",
     {"url": SMOKE_URL + "?other"}, lambda s, b: expect(s, b, 200, False)),
    ("stream_blacklists lists the blacklisted url", None, "GET", "/rpc/stream_blacklists", None,
     expect_blacklisted),
    ("sync_status reports the stream source identity", None, "GET", "/rpc/sync_status", None, expect_identity),
    ("stream_urls returns the seeded url at its block", seed_streams, "GET",
     f"/rpc/stream_urls?from_block={URL_BLOCK}&to_block={URL_BLOCK}", None,
     expect_contains({"first_block": URL_BLOCK, "md5": md5(SMOKE_URL), "url": SMOKE_URL})),
    ("stream_url_authors returns the seeded association", None, "GET",
     f"/rpc/stream_url_authors?from_block={URL_BLOCK}&to_block={URL_BLOCK}", None,
     expect_contains({"first_block": URL_BLOCK, "url_md5": md5(SMOKE_URL), "account_name": SMOKE_ACCOUNT})),
    ("stream_profiles returns the seeded profile", None, "GET",
     f"/rpc/stream_profiles?from_block={PROFILE_BLOCK}&to_block={PROFILE_BLOCK}", None,
     lambda s, b: expect(s, b, 200, [{"updated_block": PROFILE_BLOCK, "account_name": SMOKE_ACCOUNT,
                                     "avatar_url": AVATAR, "cover_url": COVER}])),
    ("stream_accounts returns the seeded account with its hive_id", None, "GET", "/rpc/stream_accounts?after_id=0", None,
     expect_smoke_account),
    ("stream_urls pages of limit=2 cover the 5 urls of one block exactly once", None, page_urls, "/rpc/stream_urls", None,
     lambda s, b: expect(s, sorted(r["md5"] for r in b), 200, sorted(md5(u) for u in PAGE_URLS))),
    ("stream_urls rejects to_block below from_block", None, "GET", "/rpc/stream_urls?from_block=10&to_block=9", None,
     lambda s, b: expect(s, b, 400)),
    ("stream_profiles serves a changed avatar at its new block", change_avatar, "GET",
     f"/rpc/stream_profiles?from_block={CHANGE_BLOCK}&to_block={CHANGE_BLOCK}", None,
     lambda s, b: expect(s, b, 200, [{"updated_block": CHANGE_BLOCK, "account_name": SMOKE_ACCOUNT,
                                     "avatar_url": AVATAR2, "cover_url": COVER}])),
    ("avatar returns the changed url", None, "GET", f"/rpc/avatar?name={SMOKE_ACCOUNT}", None, expect_url(AVATAR2)),
    ("cover returns the cover url", None, "GET", f"/rpc/cover?name={SMOKE_ACCOUNT}", None, expect_url(COVER)),
    ("avatar of an account without one is null", None, "GET", f"/rpc/avatar?name={PLAIN_ACCOUNT}", None, expect_url(None)),
    ("cover of an account without one is null", None, "GET", f"/rpc/cover?name={PLAIN_ACCOUNT}", None, expect_url(None)),
    ("avatar of an unknown account is 404", None, "GET", f"/rpc/avatar?name={UNKNOWN_ACCOUNT}", None,
     lambda s, b: expect(s, b, 404)),
    ("cover of an unknown account is 404", None, "GET", f"/rpc/cover?name={UNKNOWN_ACCOUNT}", None,
     lambda s, b: expect(s, b, 404)),
]


def write_junit(path: str, results) -> None:
    body = []
    for name, seconds, error in results:
        head = f"<testcase classname=\"api-smoke\" name={quoteattr(name)} time=\"{seconds:.3f}\">"
        body.append(f"{head}<failure message={quoteattr(error)}>{escape(error)}</failure></testcase>" if error else f"{head}</testcase>")
    failures = sum(1 for r in results if r[2])
    with open(path, "w", encoding="utf-8") as handle:
        handle.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        handle.write(f"<testsuite name=\"api-smoke\" tests=\"{len(results)}\" failures=\"{failures}\" errors=\"0\" skipped=\"0\">\n")
        handle.write("\n".join(body) + "\n</testsuite>\n")


def main(out_dir: str) -> int:
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ,
               PGRST_DB_URI=USER_URL,
               PGRST_DB_SCHEMAS="proxy_whitelist_endpoints",
               PGRST_DB_ANON_ROLE="proxy_whitelist_user",
               PGRST_DB_ROOT_SPEC="root",
               PGRST_DB_EXTRA_SEARCH_PATH="proxy_whitelist_app",
               PGRST_SERVER_HOST="127.0.0.1",
               PGRST_SERVER_PORT=str(port))
    log = open(os.path.join(out_dir, "postgrest.log"), "w", encoding="utf-8")
    proc = subprocess.Popen(["postgrest"], env=env, stdout=log, stderr=subprocess.STDOUT)
    results = []
    try:
        deadline = time.time() + STARTUP_TIMEOUT_S
        while True:
            try:
                status, _ = call(base, "GET", "/rpc/sync_status")
                if status < 500:
                    break
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            if proc.poll() is not None or time.time() > deadline:
                print(f"postgrest did not come up on {base}; see postgrest.log", file=sys.stderr)
                results.append(("postgrest start", 0.0, "postgrest did not start"))
                write_junit(os.path.join(out_dir, "api-smoke.xml"), results)
                return 1
            time.sleep(0.5)
        for name, setup, method, path, body, check in CHECKS:
            t0 = time.time()
            error = ""
            try:
                if setup:
                    setup()
                status, answer = method(base) if callable(method) else call(base, method, path, body)
                check(status, answer)
                print(f"ok   {name}: {path} -> {status} {json.dumps(answer)[:120]}")
            except Exception as exc:  # noqa: BLE001 - every failure becomes a junit case
                error = f"{type(exc).__name__}: {exc}"
                print(f"FAIL {name}: {error}", file=sys.stderr)
            results.append((name, time.time() - t0, error))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        try:
            cleanup()
        except subprocess.CalledProcessError as exc:
            print(f"cleanup failed: {exc.stderr}", file=sys.stderr)
    write_junit(os.path.join(out_dir, "api-smoke.xml"), results)
    return 1 if any(r[2] for r in results) else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} OUT_DIR")
    sys.exit(main(sys.argv[1]))
