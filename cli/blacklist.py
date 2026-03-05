#!/usr/bin/env python3
"""
Account blacklisting CLI for proxy_whitelist.

Blacklists a Hive account and optionally un-whitelists their URLs.
Connects directly to PostgreSQL — requires the proxy_whitelist_app schema.

Usage:
    python -m cli.blacklist <account> [options]

Examples:
    python -m cli.blacklist spammer --mode skip
    python -m cli.blacklist spammer --mode remove-unique
    python -m cli.blacklist spammer --mode remove-all --also-blacklist-urls
    python -m cli.blacklist spammer --mode remove-all --dry-run
"""

import argparse
import logging
import sys

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("blacklist")


def blacklist_account(
    conn,
    account: str,
    mode: str,
    also_blacklist_urls: bool,
    dry_run: bool,
) -> None:
    """Blacklist an account and optionally un-whitelist their URLs."""

    log.info("Blacklisting account: %s (mode=%s, also_blacklist_urls=%s, dry_run=%s)",
             account, mode, also_blacklist_urls, dry_run)

    cur = conn.cursor()

    # Look up account
    cur.execute(
        "SELECT id FROM proxy_whitelist_app.accounts WHERE name = %s", [account]
    )
    row = cur.fetchone()
    if row is None:
        log.error("Account %s not found in proxy_whitelist_app.accounts", account)
        sys.exit(1)
    account_id = row[0]

    # Check if already blacklisted
    cur.execute(
        "SELECT 1 FROM proxy_whitelist_app.blacklisted_accounts WHERE account_id = %s",
        [account_id],
    )
    if cur.fetchone():
        log.warning("Account %s is already blacklisted", account)

    if mode == "skip":
        if not dry_run:
            cur.execute(
                "INSERT INTO proxy_whitelist_app.blacklisted_accounts (account_id) "
                "VALUES (%s) ON CONFLICT DO NOTHING",
                [account_id],
            )
            conn.commit()
        log.info("Account %s blacklisted (no URL changes)", account)
        return

    # Get all URLs posted by this account
    cur.execute(
        """SELECT u.id, u.url
           FROM proxy_whitelist_app.url_authors ua
           JOIN proxy_whitelist_app.urls u ON u.id = ua.url_id
           WHERE ua.author_id = %s""",
        [account_id],
    )
    account_urls = cur.fetchall()
    log.info("Account %s has %d whitelisted URLs", account, len(account_urls))

    removed_urls = []
    kept_urls = []

    for url_id, url in account_urls:
        if mode == "remove-all":
            removed_urls.append((url_id, url))

        elif mode == "remove-unique":
            # Only remove if no other non-blacklisted account posted it
            cur.execute(
                """SELECT a.name
                   FROM proxy_whitelist_app.url_authors ua2
                   JOIN proxy_whitelist_app.accounts a ON a.id = ua2.author_id
                   WHERE ua2.url_id = %s
                     AND ua2.author_id != %s
                     AND ua2.author_id NOT IN (
                         SELECT account_id FROM proxy_whitelist_app.blacklisted_accounts
                     )""",
                [url_id, account_id],
            )
            remaining = [r[0] for r in cur.fetchall()]

            if not remaining:
                removed_urls.append((url_id, url))
            else:
                kept_urls.append((url_id, url))
                log.info("  KEEP %s (also posted by: %s)", url, ", ".join(remaining))

    # Apply changes
    for url_id, url in removed_urls:
        log.info("  REMOVE %s", url)
        if not dry_run:
            if also_blacklist_urls:
                # Keep URL in urls table (is_url_blacklisted needs it for lookup)
                # but add to permanent blacklist and remove author association
                cur.execute(
                    "INSERT INTO proxy_whitelist_app.blacklisted_urls (url_id) "
                    "VALUES (%s) ON CONFLICT DO NOTHING",
                    [url_id],
                )
                cur.execute(
                    "DELETE FROM proxy_whitelist_app.url_authors "
                    "WHERE url_id = %s AND author_id = %s",
                    [url_id, account_id],
                )
                log.info("  BLACKLIST URL %s", url)
            else:
                # Delete the URL entirely (CASCADE removes url_authors rows)
                cur.execute(
                    "DELETE FROM proxy_whitelist_app.urls WHERE id = %s", [url_id]
                )

    if not dry_run:
        # Add to blacklisted accounts
        cur.execute(
            "INSERT INTO proxy_whitelist_app.blacklisted_accounts (account_id) "
            "VALUES (%s) ON CONFLICT DO NOTHING",
            [account_id],
        )
        conn.commit()

    log.info("Done: %d URLs removed, %d URLs kept, account %s blacklisted%s",
             len(removed_urls), len(kept_urls), account,
             " (DRY RUN)" if dry_run else "")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Blacklist a Hive account in the proxy whitelist system.",
    )
    parser.add_argument("account", help="Hive account name to blacklist")
    parser.add_argument(
        "--mode",
        choices=["skip", "remove-all", "remove-unique"],
        default="skip",
        help=(
            "skip: only blacklist account, no URL changes. "
            "remove-all: un-whitelist all URLs from this account. "
            "remove-unique: only un-whitelist URLs not posted by other good accounts."
        ),
    )
    parser.add_argument(
        "--also-blacklist-urls",
        action="store_true",
        help="Also add removed URLs to the URL blacklist (permanently blocked).",
    )
    parser.add_argument(
        "--postgres-url",
        default="postgresql:///haf_block_log",
        help="PostgreSQL connection URL (default: postgresql:///haf_block_log)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes.",
    )

    args = parser.parse_args()

    try:
        conn = psycopg2.connect(args.postgres_url)
    except psycopg2.OperationalError as e:
        log.error("Cannot connect to PostgreSQL at %s: %s", args.postgres_url, e)
        sys.exit(1)

    try:
        blacklist_account(
            conn=conn,
            account=args.account,
            mode=args.mode,
            also_blacklist_urls=args.also_blacklist_urls,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
