"""Checks of the repository itself: what the installer applies, what the public
URL rewriter points at, and the line endings .gitattributes promises. No database."""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
INSTALL = (ROOT / "scripts" / "install_app.sh").read_text()
ENDPOINT_SQL = "\n".join(p.read_text() for p in sorted((ROOT / "endpoints").glob("*.sql")))


def sql_files() -> list[str]:
    return sorted(str(p.relative_to(ROOT)) for d in ("db", "endpoints") for p in (ROOT / d).glob("*.sql"))


def test_every_sql_file_is_applied_by_install_app() -> None:
    missing = [f for f in sql_files() if f'$SRCPATH/{f}"' not in INSTALL]
    assert not missing, f"not applied by scripts/install_app.sh: {missing}"


def test_replica_install_applies_every_replica_sql_file_and_no_stream_endpoints() -> None:
    replica_install = (ROOT / "replica" / "install.sh").read_text()
    files = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "replica" / "db").glob("*.sql"))
    missing = [f for f in files if f'$SRCPATH/{f}"' not in replica_install]
    assert not missing, f"not applied by replica/install.sh: {missing}"
    # A replica is not a stream source, and its sync_status() replaces the HAF one.
    for primary_only in ("endpoints/stream.sql", "endpoints/sync_status.sql"):
        assert primary_only not in replica_install, f"replica/install.sh applies {primary_only}"


def test_rewrite_rules_point_at_installed_endpoint_functions() -> None:
    rules = (ROOT / "rewrite_rules.conf").read_text()
    targets = re.findall(r"\s/rpc/([a-z_]+)[\s?]", rules)
    assert targets, "rewrite_rules.conf names no /rpc/ targets"
    defined = set(re.findall(r"FUNCTION\s+proxy_whitelist_endpoints\.([a-z_]+)", ENDPOINT_SQL))
    missing = [t for t in targets if t not in defined]
    assert not missing, f"rewrite targets without an endpoint function: {missing}"


def test_check_url_accepts_the_callers_account_blacklist() -> None:
    # proxy-whitelist!4: the imagehoster sends blacklisted_accounts with every check.
    assert re.search(r'check_url\(\s*"url"\s+TEXT\s*,\s*"blacklisted_accounts"\s+TEXT\[\]', ENDPOINT_SQL), \
        "check_url(url text, blacklisted_accounts text[]) is not declared in endpoints/whitelist_check.sql"


def test_ci_shellcheck_runs_at_the_gates_floor() -> None:
    # .aidev/run-checks.sh lints at -S warning; a CI default-severity run fails on
    # info findings the gate never sees (SC1091 broke the handoff pipeline once).
    ci = (ROOT / ".gitlab-ci.yml").read_text()
    job = re.search(r"^lint_bash_scripts:\n((?:[ \t].*\n|\n)*)", ci, re.MULTILINE)
    assert job, "no lint_bash_scripts job in .gitlab-ci.yml"
    calls = re.findall(r"shellcheck\s[^|\n]*", job.group(1))
    assert calls, "lint_bash_scripts does not run shellcheck"
    for call in calls:
        flags = call.split()
        assert "-x" in flags, f"lint_bash_scripts shellcheck does not follow sources: {call}"
        assert re.search(r"(^|\s)-S warning(\s|$)", call), f"lint_bash_scripts shellcheck is not at -S warning: {call}"
    for xslt in re.findall(r"xmlstarlet tr (\S+)", job.group(1)):
        assert (ROOT / xslt).is_file(), f"lint_bash_scripts converts with a missing {xslt}"


def test_tracked_text_files_use_lf_line_endings() -> None:
    # .gitattributes: * text=auto eol=lf
    crlf = []
    for p in ROOT.rglob("*"):
        if not p.is_file() or ".git" in p.parts or "test-results" in p.parts or p.suffix in {".png", ".jpg", ".gif", ".pyc"}:
            continue
        try:
            data = p.read_bytes()
        except OSError:
            continue
        if b"\x00" in data[:1024]:
            continue
        if b"\r\n" in data:
            crlf.append(str(p.relative_to(ROOT)))
    assert not crlf, f"CRLF line endings in: {crlf}"
