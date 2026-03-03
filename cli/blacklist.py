#!/usr/bin/env python3
"""
Account blacklisting CLI for proxy_whitelist.

Blacklists a Hive account and optionally un-whitelists their URLs.
Connects directly to Redis — does not require HAF.

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

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("blacklist")


def blacklist_account(
    client: redis.Redis,
    account: str,
    mode: str,
    also_blacklist_urls: bool,
    dry_run: bool,
) -> None:
    """Blacklist an account and optionally un-whitelist their URLs."""

    log.info("Blacklisting account: %s (mode=%s, also_blacklist_urls=%s, dry_run=%s)",
             account, mode, also_blacklist_urls, dry_run)

    # Check if already blacklisted
    if client.sismember("proxy:whitelist:blacklisted_accounts", account):
        log.warning("Account %s is already blacklisted", account)

    if mode == "skip":
        if not dry_run:
            client.sadd("proxy:whitelist:blacklisted_accounts", account)
        log.info("Account %s blacklisted (no URL changes)", account)
        return

    # Get all URLs posted by this account
    account_key = f"proxy:whitelist:account_urls:{account}"
    urls = client.smembers(account_key)
    log.info("Account %s has %d whitelisted URLs", account, len(urls))

    removed_urls = []
    kept_urls = []

    for url_bytes in urls:
        url = url_bytes.decode("utf-8") if isinstance(url_bytes, bytes) else url_bytes
        url_accounts_key = f"proxy:whitelist:url_accounts:{url}"

        if mode == "remove-all":
            # Remove URL from whitelist regardless of other posters
            removed_urls.append(url)

        elif mode == "remove-unique":
            # Only remove if no other non-blacklisted account posted it
            other_accounts = client.smembers(url_accounts_key)
            blacklisted = client.smembers("proxy:whitelist:blacklisted_accounts")
            # Add current account to blacklisted set for this check
            blacklisted.add(account.encode("utf-8") if isinstance(account, str) else account)

            remaining = set()
            for acct in other_accounts:
                if acct not in blacklisted:
                    remaining.add(acct)

            if not remaining:
                removed_urls.append(url)
            else:
                kept_urls.append(url)
                log.info("  KEEP %s (also posted by: %s)", url,
                         ", ".join(a.decode("utf-8") if isinstance(a, bytes) else a
                                  for a in remaining))

    # Apply changes
    for url in removed_urls:
        log.info("  REMOVE %s", url)
        if not dry_run:
            pipe = client.pipeline()
            pipe.srem("proxy:whitelist:urls", url)
            pipe.srem(f"proxy:whitelist:url_accounts:{url}", account)
            if also_blacklist_urls:
                pipe.sadd("proxy:whitelist:url_blacklist", url)
                log.info("  BLACKLIST URL %s", url)
            pipe.execute()

    if not dry_run:
        # Remove the account's URL set
        client.delete(account_key)
        # Add to blacklisted accounts
        client.sadd("proxy:whitelist:blacklisted_accounts", account)

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
        "--redis-url",
        default="redis://localhost:6379",
        help="Redis URL (default: redis://localhost:6379)",
    )
    parser.add_argument(
        "--redis-password",
        default=None,
        help="Redis password (optional, for ACL auth)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes.",
    )

    args = parser.parse_args()

    client = redis.from_url(args.redis_url, password=args.redis_password)

    try:
        client.ping()
    except redis.ConnectionError as e:
        log.error("Cannot connect to Redis at %s: %s", args.redis_url, e)
        sys.exit(1)

    blacklist_account(
        client=client,
        account=args.account,
        mode=args.mode,
        also_blacklist_urls=args.also_blacklist_urls,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
