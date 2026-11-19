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
  url_accounts of the seeded URL.

Writes OUT_DIR/api-smoke.xml (one junit case per call) and exits non-zero when any
call fails. The seeded rows are removed at the end.
"""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from xml.sax.saxutils import escape, quoteattr

STARTUP_TIMEOUT_S = 60
ADMIN_URL = os.environ["PROXY_WHITELIST_TEST_ADMIN_URL"]
USER_URL = os.environ["PROXY_WHITELIST_TEST_USER_URL"]
SMOKE_URL = f"https://aidev.invalid/smoke/{int(time.time())}.png"
SMOKE_ACCOUNT = "aidev-smoke"


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
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, raw


def seed_url() -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        insert into proxy_whitelist_app.urls (url) values ('{SMOKE_URL}');
        insert into proxy_whitelist_app.accounts (hive_id, name) values (2147000001, '{SMOKE_ACCOUNT}')
            on conflict (name) do nothing;
        insert into proxy_whitelist_app.url_authors (url_id, author_id)
            select u.id, a.id from proxy_whitelist_app.urls u, proxy_whitelist_app.accounts a
            where u.url = '{SMOKE_URL}' and a.name = '{SMOKE_ACCOUNT}';
    """)


def blacklist_url() -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        insert into proxy_whitelist_app.blacklisted_urls (url_id)
            select id from proxy_whitelist_app.urls where url = '{SMOKE_URL}';
    """)


def cleanup() -> None:
    psql(f"""
        set role proxy_whitelist_owner;
        delete from proxy_whitelist_app.blacklisted_urls
            where url_id in (select id from proxy_whitelist_app.urls where url = '{SMOKE_URL}');
        delete from proxy_whitelist_app.url_authors
            where url_id in (select id from proxy_whitelist_app.urls where url = '{SMOKE_URL}');
        delete from proxy_whitelist_app.urls where url = '{SMOKE_URL}';
        delete from proxy_whitelist_app.accounts where name = '{SMOKE_ACCOUNT}';
    """)


def expect(status, body, want_status, want_body=None, want_type=None):
    assert status == want_status, f"HTTP {status}: {body!r}"
    if want_body is not None:
        assert body == want_body, f"expected {want_body!r}, got {body!r}"
    if want_type is not None:
        assert isinstance(body, want_type), f"expected {want_type.__name__}, got {body!r}"


# (name, setup or None, method, path, body, check)
CHECKS = [
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
     lambda s, b: (expect(s, b, 200), (lambda: None if SMOKE_ACCOUNT in json.dumps(b) else (_ for _ in ()).throw(AssertionError(f"{SMOKE_ACCOUNT} not in {b!r}")))())),
    ("check_url blacklisted after blacklisting the url", blacklist_url, "POST", "/rpc/check_url", {"url": SMOKE_URL},
     lambda s, b: expect(s, b, 200, "blacklisted")),
    ("is_whitelisted of an unseen url is false", None, "POST", "/rpc/is_whitelisted",
     {"url": SMOKE_URL + "?other"}, lambda s, b: expect(s, b, 200, False)),
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
                status, answer = call(base, method, path, body)
                check(status, answer)
                print(f"ok   {name}: {method} {path} -> {status} {json.dumps(answer)[:120]}")
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
