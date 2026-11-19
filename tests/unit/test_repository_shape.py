"""Checks of the repository itself: what the installer applies, what the public
URL rewriter points at, and the line endings .gitattributes promises. No database."""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
INSTALL = (ROOT / "scripts" / "install_app.sh").read_text()
ENDPOINT_SQL = "\n".join(p.read_text() for p in sorted((ROOT / "endpoints").glob("*.sql")))


def sql_files():
    return sorted(str(p.relative_to(ROOT)) for d in ("db", "endpoints") for p in (ROOT / d).glob("*.sql"))


def test_every_sql_file_is_applied_by_install_app():
    missing = [f for f in sql_files() if f'$SRCPATH/{f}"' not in INSTALL]
    assert not missing, f"not applied by scripts/install_app.sh: {missing}"


def test_rewrite_rules_point_at_installed_endpoint_functions():
    rules = (ROOT / "rewrite_rules.conf").read_text()
    targets = re.findall(r"\s/rpc/([a-z_]+)[\s?]", rules)
    assert targets, "rewrite_rules.conf names no /rpc/ targets"
    defined = set(re.findall(r"FUNCTION\s+proxy_whitelist_endpoints\.([a-z_]+)", ENDPOINT_SQL))
    missing = [t for t in targets if t not in defined]
    assert not missing, f"rewrite targets without an endpoint function: {missing}"


def test_check_url_accepts_the_callers_account_blacklist():
    # proxy-whitelist!4: the imagehoster sends blacklisted_accounts with every check.
    assert re.search(r'check_url\(\s*"url"\s+TEXT\s*,\s*"blacklisted_accounts"\s+TEXT\[\]', ENDPOINT_SQL), \
        "check_url(url text, blacklisted_accounts text[]) is not declared in endpoints/whitelist_check.sql"


def test_tracked_text_files_use_lf_line_endings():
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
