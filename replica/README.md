# proxy-whitelist replica

A read-only copy of a proxy-whitelist primary that runs next to an image server,
so that the imagehoster's whitelist checks and avatar lookups are answered
locally instead of across the network. It serves the primary's own API
(`/whitelist/check`, `/whitelist/url-accounts`, `/avatar/{name}`, `/cover/{name}`,
`/sync-status`, …) from replicated tables:

- **replica-db** — a plain Postgres (no HAF) holding `proxy_whitelist_app`'s
  tables (`replica/db/schema.sql`) and the syncer's state
  (`proxy_whitelist_replica.state`).
- **replica-install** — one shot: `replica/install.sh` creates the tables, then
  installs the repository's endpoint functions (`endpoints/*.sql`, unchanged)
  over them, and the replica's `sync_status()` (`replica/db/replica_sync_status.sql`).
  The `/stream/*` functions are not installed: a replica is not a source.
- **replica-sync** — `replica/sync.py`, following the primary's replication
  streams (contract v2, `endpoints/stream.sql`).
- **replica-postgrest** and **replica-rewriter** — PostgREST on the endpoint
  schema behind the same URL rewriter as the primary, so paths match.

One primary serves any number of replicas. The primary keeps no state about
them: each replica pulls.

## Deploy

```sh
cp replica/.env.example replica/.env      # set PROXY_WHITELIST_SOURCE_URL and the passwords
docker compose -f replica/compose.yml --env-file replica/.env up -d
```

`PROXY_WHITELIST_SOURCE_URL` is the primary's API base as reachable from the
replica host, e.g. `http://10.10.10.8/proxy-whitelist-api`. The API listens on
`REPLICA_HTTP_BIND` (default `127.0.0.1:7010`), PostgREST's admin server
(`/live`, `/ready`) on `REPLICA_POSTGREST_ADMIN_BIND` (default `127.0.0.1:3001`).
The images default to the latest published ones; `docker compose build` builds
them from this checkout instead.

## Syncing

Every `POLL_INTERVAL_S` (default 5) the syncer reads the primary's
`/sync-status`, pages `/stream/accounts`, `/stream/urls`, `/stream/url-authors`
and `/stream/profiles` from its cursors up to the primary's `last_block_num`
(`PAGE_LIMIT` rows per page, default and maximum 100000; CSV when the primary
offers it), replaces both blacklists from `/stream/blacklists`, and then records
that block as replicated. Each page is applied together with its cursor in one
transaction: a failure (HTTP error, timeout, undecodable page, constraint
violation) is logged with the stream and cursor, sets `sync_error` and is
retried after 3 s, doubling up to 60 s, from the last applied page.

Rows are keyed by natural keys (account `hive_id` and name, `md5(url)`,
(url, account), profile by account); the replica's ids are its own. A url
author can arrive before its url (its block is the last edit of the post, which
can be older than the url's own block): it waits in
`proxy_whitelist_replica.pending_url_authors` until both its url and its account
are there.

Blacklisted urls or accounts that the streams have not delivered yet are left
out of the replica's lists until a later cycle delivers them; operator removals
on the primary (`cli/blacklist.py remove-all`) are not replicated (the streams do
not report deletions).

### Bootstrap

A new replica runs the same cycle from block 0. On steem-8's data (about 140 M
rows) expect the bootstrap to take hours rather than minutes; it has not been
measured yet, so time the first one. The syncer can be stopped and restarted at
any point: it continues from the last applied page. Until the first full
catch-up `bootstrap_complete` is `false` and `last_block_num`/`last_block_time`
are `null`, so the replica reads as down.

### Source identity, `--reset`

On first contact the replica stores the primary's `sync_uuid` and
`stream_schema_version`. The primary re-mints `sync_uuid` when its data is
rebuilt (e.g. a HAF replay renumbering its rows). When either differs from the
stored one, the syncer writes nothing, sets `sync_error` and exits with status 2;
the replica keeps answering from its existing data and its `last_block_time`
freezes, so it is soon judged down. To accept the new source, run the syncer
once with `--reset` (or set `PROXY_WHITELIST_REPLICA_RESET=1` and restart
`replica-sync`): it truncates the replicated tables and cursors and bootstraps
again.

```sh
docker compose -f replica/compose.yml --env-file replica/.env run --rm replica-sync replica_sync --reset --once
```

## Health

`/sync-status` (`proxy_whitelist_replica.sync_status()`) has the primary's
fields, with the values of the primary the replica has **fully** replicated:

| field | meaning |
| --- | --- |
| `last_block_num`, `last_block_time` | the primary's last processed block and its time, as of the last cycle that applied every stream |
| `sync_uuid`, `stream_schema_version`, `stream_sentinel_block` | the primary's, as stored on first contact |
| `source_sync_uuid`, `source_url` | the source being followed |
| `polled_at` | when the last full cycle finished |
| `bootstrap_complete` | the first full catch-up is done |
| `sync_error` | the last failure, cleared by the next full cycle |

`last_block_time` stops advancing whenever the primary stalls, the link to it
drops, the syncer dies or the source identity changes. haf_api_node's stock
`check_sync_status` agent (`healthchecks/checks/check_http_sync_age.sh`: the
PostgREST `/live` check, then the age of `last_block_time` against a 60 s
threshold) therefore judges a replica exactly as it judges the primary: up when
in sync, down within a minute of the primary or the syncer stopping. A quiet
hour without new image urls does not make it stale, since every cycle advances
to the primary's block even with no rows.

## Fronting it with haproxy (next step)

The image server's haproxy is to route the imagehoster's `whitelist.apiUrl`
(`http://127.0.0.1`) to this replica's rewriter, with the primary as a `backup`
server, both judged by the agent above: a down replica degrades to the primary
rather than failing open. That routing, and pointing the imagehoster at it, are
the next issue; this profile only provides the replica.
