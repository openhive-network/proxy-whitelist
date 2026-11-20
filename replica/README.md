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
- **replica-agent** — haproxy's agent check for the replica
  (`replica/healthcheck/proxy_whitelist_replica.sh`, see [Health](#health)).

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
(`/live`, `/ready`) on `REPLICA_POSTGREST_ADMIN_BIND` (default `127.0.0.1:3001`),
the agent check on `REPLICA_AGENT_BIND` (default `127.0.0.1:7015`) and the
replica database on `127.0.0.1:${REPLICA_DB_PORT}` (default 5433). The images
default to the latest published ones; `docker compose build` builds them from
this checkout instead.

The syncer needs psycopg 3 in the proxy-whitelist image. The image's base
(common-ci-configuration's `psql`, Alpine 3.16) has no `py3-psycopg` package
(`apk add py3-psycopg` fails there), so the `Dockerfile` installs
`psycopg[binary]` from PyPI instead; the first build of that image has to
confirm that the wheel installs and `python3 -c 'import psycopg'` works in it.

## Syncing

Every `POLL_INTERVAL_S` (default 5) the syncer reads the primary's
`/sync-status`, pages `/stream/accounts`, `/stream/urls`, `/stream/url-authors`
and `/stream/profiles` from its cursors up to the primary's `last_block_num`
(`PAGE_LIMIT` rows per page, default and maximum 100000), replaces both
blacklists from `/stream/blacklists`, and then records that block as replicated.
Each page is applied together with its cursor in one transaction: a failure
(HTTP error, timeout, undecodable page — including a corrupt gzip body —,
constraint violation) is logged with the stream and cursor, sets `sync_error`
and is retried after 3 s, doubling up to 60 s, from the last applied page.

Pages are requested and read as `application/json` only. PostgREST's `text/csv`
renders each value as its JSON encoding with the quotes removed, so a url with
a backslash, a double quote or a control character arrives altered (one `\`
becomes `\\`); the url md5 check rejects such a page and a profile url would be
stored corrupted. That encoding is not documented, so CSV is not used at all.

Every request sends `Accept-Encoding: gzip`; a `Content-Encoding: gzip` answer
(caddy's `encode`) is decompressed before it is parsed, and an uncompressed
answer is read as is. A urls page of 100000 rows is about 17 MB as JSON and
about 6 MB compressed. The first stream page of a process logs at INFO whether
the source compresses (`the source compresses stream pages` or `the source does
not compress stream pages`), so a slow bootstrap over the WAN can be told apart
from one whose path strips compression.

### Page budget

A page has to arrive within the shortest timeout on the path to the primary's
PostgREST: caddy → haproxy (`timeout server 30s`) → the URL rewriter
(`proxy_read_timeout 60s`) → PostgREST; the syncer itself waits 300 s. A
100000-row `/stream/url-authors` page over a dense block (the sentinel block)
can take longer than 30 s on steem-8, and haproxy answers 504. On a 502, a 504
or a timeout the syncer retries that page at once, from the same cursor, with
half the stream's page size (down to 1000, or `PAGE_LIMIT` when that is lower),
logging `retrying with page_size N instead of M`; after a page passes it goes
back to `PAGE_LIMIT`. A page that still fails at 1000 rows is an ordinary
failure (`sync_error`, backoff). When the log shows the halving on most pages,
lower `PAGE_LIMIT` (e.g. 25000) instead of paying a timeout per page.

Rows are keyed by natural keys (account `hive_id` and name, `md5(url)`,
(url, account), profile by account); the replica's ids are its own. A url
author can arrive before its url (its block is the last edit of the post, which
can be older than the url's own block): it waits in
`proxy_whitelist_replica.pending_url_authors` until both its url and its account
are there.

Blacklisted urls or accounts that the streams have not delivered yet are left
out of the replica's lists until a later cycle delivers them. Rows the operator
deletes on the primary (`cli/blacklist.py remove-all`) are not replicated: the
streams do not report deletions. After such a removal, force a rebuild of every
replica ([`--rebuild`](#forced-rebuild---rebuild)).

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
stored one, the syncer writes nothing, sets `sync_error` (once, naming both
identities) and logs it once; the replica keeps answering from its existing
data and its `last_block_time` freezes, so it is judged down within a minute.
The syncer does not exit: it reads the primary's `/sync-status` again every
`POLL_INTERVAL_S` and, if the old identity returns (e.g. the primary was pointed
back at its previous database), resumes on the next poll without a restart.
With `--once` it exits with status 2 instead.

To accept the new source, run the syncer once with `--reset` (or set
`PROXY_WHITELIST_REPLICA_RESET=1` and restart `replica-sync`): it truncates the
replicated tables and cursors and bootstraps again.

```sh
docker compose -f replica/compose.yml --env-file replica/.env run --rm replica-sync replica_sync --reset --once
```

### Forced rebuild, `--rebuild`

`--reset` acts only on a changed identity. To rebuild against an unchanged
primary — after `cli/blacklist.py remove-all` there, or when the replica is
suspected to have diverged — use `--rebuild`: before its first cycle the syncer
truncates the replicated tables and cursors in one transaction (whatever the
source identity; the new one is adopted) and bootstraps again. Meanwhile
`/sync-status` has `bootstrap_complete` false and no `last_block_time`, so the
agent answers down and haproxy sends the traffic to the primary.

```sh
docker compose -f replica/compose.yml --env-file replica/.env stop replica-sync
docker compose -f replica/compose.yml --env-file replica/.env run --rm replica-sync replica_sync --rebuild --once
docker compose -f replica/compose.yml --env-file replica/.env start replica-sync
```

Or set `PROXY_WHITELIST_REPLICA_REBUILD=1` in `replica/.env` and recreate
`replica-sync` (`up -d`). The variable is consumed once: the syncer records in
`proxy_whitelist_replica.state` that it rebuilt for it, and later starts with it
still set (a container restart) do not rebuild again. The record is cleared by
a start with the variable unset, so set it back to `0` afterwards; the next `1`
rebuilds again. Do not put `--rebuild` in the service's `command`: with
`restart: always` every restart would rebuild.

### Restarts

Without `--once` the syncer exits only on a configuration error (a missing or
invalid variable, exit 1); network, database and identity problems are waited
out in the process, so `restart: always` does not turn them into a restart
loop.

## Health

`/sync-status` (`proxy_whitelist_replica.sync_status()`) has the primary's
fields, with the values of the primary the replica has **fully** replicated:

| field | meaning |
| --- | --- |
| `last_block_num`, `last_block_time` | the primary's last processed block and its time, as of the last cycle that applied every stream |
| `sync_uuid` | the replica's own identity, minted when its state is created and again by every `--reset` or `--rebuild` (the replica serves no streams, so it is not a source identity) |
| `stream_schema_version`, `stream_sentinel_block` | the primary's, as stored on first contact |
| `source_sync_uuid`, `source_url` | the source being followed: its `sync_uuid` as stored on first contact |
| `polled_at` | when the last full cycle finished |
| `bootstrap_complete` | the first full catch-up is done |
| `sync_error` | the last failure, cleared by the next full cycle |

`last_block_time` stops advancing whenever the primary stalls, the link to it
drops, the syncer dies or the source identity changes. A quiet hour without new
image urls does not make it stale, since every cycle advances to the primary's
block even with no rows.

### The agent check

haf_api_node's stock `healthchecks/checks/proxy_whitelist.sh` cannot judge a
replica: it first runs `check_haf_lib`, which queries `hive.is_instance_ready()`,
`hafd.blocks` and `hafd.hive_state`, so on the replica's plain Postgres it
answers `down #HAF query failed` whatever the sync state.
`replica/healthcheck/proxy_whitelist_replica.sh` is that script without the HAF
gate: haf_api_node's `check_sync_status` (PostgREST's `/live`, then
`proxy_whitelist_endpoints.sync_status()` called with psql as
`proxy_whitelist_user`, and the age of `last_block_time` against 60 s). It
prints `up`, or `down #<reason>` and exits 10, like the stock agents, so
haproxy's `agent-check` uses it unchanged: up when in sync, down within a minute
of the primary or the syncer stopping, during a bootstrap or rebuild, and when
PostgREST is not live. It never queries `hive.*` or `hafd.*`.

haf_api_node's `check_http_sync_age.sh` and `format_seconds.sh` are
**vendored** next to it (unchanged, from haf_api_node `develop` at `0a619f2e`),
so the script runs from the proxy-whitelist image without haf_api_node; update
them from there when haf_api_node changes them. They are written for busybox
(`date -d "YYYY.MM.DD-HH:MM:SS"`), which the image has.

The **replica-agent** service runs it from the proxy-whitelist image, one check
per connection (`nc -lk -p 7015 -e …`), on `REPLICA_AGENT_BIND`, reaching
PostgREST and the database over the compose network:

```
server replica 127.0.0.1:7010 check agent-check agent-port 7015 agent-inter 5s
```

To run it from the host instead (a cron job, or an xinetd/socat service on the
agent port), use the published database port, e.g.:

```sh
REPLICA_LIVE_URL=http://127.0.0.1:3001/live \
REPLICA_POSTGRES_URL=postgresql://proxy_whitelist_user:$REPLICA_API_PASSWORD@127.0.0.1:5433/proxy_whitelist \
  replica/healthcheck/proxy_whitelist_replica.sh
```

On a host without busybox `date` the vendored age check cannot parse the block
time (`down #… unparseable block time`); run it in the image there.

## Fronting it with haproxy (next step)

The image server's haproxy is to route the imagehoster's `whitelist.apiUrl`
(`http://127.0.0.1`) to this replica's rewriter, with the primary as a `backup`
server, both judged by the agent check above: a down replica degrades to the primary
rather than failing open. That routing, and pointing the imagehoster at it, are
the next issue; this profile only provides the replica.
