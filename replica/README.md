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
  streams (contract v2, `endpoints/stream.sql`), and, where enabled, purging
  the imagehoster's avatar/cover redirects at Cloudflare (`replica/purge.py`,
  see [Cache invalidation](#cache-invalidation)).
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
about 6 MB compressed. The first stream page of a process that settles it logs
at INFO whether the source compresses (`the source compresses stream pages` or
`the source does not compress stream pages`), so a slow bootstrap over the WAN
can be told apart from one whose path strips compression. An uncompressed page
below 1024 bytes settles nothing (caddy leaves short answers uncompressed, e.g.
the empty `[]` pages of a resumed replica), so the line can come late, or never
when every page of the process is that small.

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

Known difference: a replica bootstrapped from a primary installed before the
placeholder fix keeps hivemind's placeholder account (`hive_id` 0, empty name)
in `proxy_whitelist_app.accounts`. The primary's install deletes the row and no
longer backfills it, but that deletion is not streamed either; the row leaves
the replica with its next rebuild. It is never looked up.

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
| `purge_enabled` | the running syncer purges Cloudflare ([Cache invalidation](#cache-invalidation)) |
| `purge_pending` | accounts in `proxy_whitelist_replica.purge_queue` waiting for their purge |
| `purge_oldest_block` | the lowest profile block among them, `null` when none waits |
| `purge_last_error` | the error of the most often failed waiting account, `null` when none failed |

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
gate: PostgREST's `/live` (haf_api_node's `check_postgrest_live`), then
`proxy_whitelist_endpoints.sync_status()` called with psql as
`proxy_whitelist_user`, and the age of `last_block_time` against 60 s
(haf_api_node's `check_http_block_time`). It
prints `up`, or `down #<reason>` and exits 10, like the stock agents, so
haproxy's `agent-check` uses it unchanged: up when in sync, down within a minute
of the primary or the syncer stopping, during a bootstrap or rebuild, and when
PostgREST is not live. It never queries `hive.*` or `hafd.*`. A Cloudflare purge
backlog keeps it `up` (a stale avatar redirect is no reason to send whitelist
traffic to the primary) with the backlog as its reason,
`up #proxy_whitelist_replica purge backlog: N accounts since block B`, which
haproxy shows in `show stat`.

haf_api_node's `check_http_sync_age.sh` and `format_seconds.sh` are
**vendored** next to it (unchanged, from haf_api_node `develop` at `0a619f2e`),
so the script runs from the proxy-whitelist image without haf_api_node; update
them from there when haf_api_node changes them. They are written for busybox
(`date -d "YYYY.MM.DD-HH:MM:SS"`), which the image has.

The sync-status query is the script's own, not `check_sync_status`'s. That one
sends `BEGIN; SET LOCAL search_path …; SELECT …; COMMIT` as one `--command`, and
psql before 15 prints only the last statement's result (`COMMIT`'s, nothing), so
with the image's psql 14 every probe read `down #… has no processed block yet`.
The script sends the `SELECT` alone, with the search_path as a startup option
(`PGOPTIONS`), which works with **any psql** (the replica's plain Postgres has no
pgbouncer to drop it). Keep any query the agent runs a single statement as long
as the image's psql is older than 15; `.aidev/replica_smoke.py health` runs every
case also with a psql that prints only the last statement's result.

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

## Cache invalidation

`images.hive.blog/u/<name>/avatar[/<size>]` and `/u/<name>/cover` are 302
redirects that Cloudflare caches for a day (imagehoster#15). When an account
changes its profile image, those cached redirects have to be purged, and only
**after** the replica the imagehoster reads has committed the new profile:
a Cloudflare miss right after an earlier purge would reach an imagehoster still
redirecting to the old image and cache that for another day.

The syncer therefore purges itself, through an outbox. In the transaction that
applies a `/stream/profiles` page (and its cursor), every account whose
`avatar_url` or `cover_url` differs from the stored one (`IS DISTINCT FROM`, so
setting, changing and removing all count) is upserted into
`proxy_whitelist_replica.purge_queue` with the profile's block and which of the
two changed. Rows that change only other fields are not queued. A second change
of the same account before its purge raises the row's block and ORs the flags:
one purge covers both. After the commit — after each page that queued
something, and at the end of every cycle so an earlier backlog drains too —
the queue is drained, lowest block first, in calls of at most 30 URLs
(`POST /zones/{zone}/purge_cache` with `files`, Cloudflare's per-request limit
outside the Enterprise plan) for at most `PURGE_TIME_BUDGET_S` (default 5 s)
per drain; a call started within the budget runs to its end or its 10 s timeout.
A purged account's row is deleted; purging is idempotent at Cloudflare, so the
rare repeat after a crash between the call and the delete is harmless
(at-least-once).

A failed call (HTTP error, connection error, an answer without `success`)
increments `attempts` and sets `last_error` on its rows, stops the drain and
defers the next one by 3 s doubling to 300 s, or by `Retry-After` on HTTP 429;
it never sets `sync_error`, never moves a cursor and never stops the sync.
Draining never writes replicated tables, and applying never calls Cloudflare.

**Bootstrap rule.** Accounts are queued only by a cycle that starts with
`bootstrap_complete` already true. A bootstrap, `--reset` or `--rebuild`
applies every historical profile and purges nothing; `--reset` and `--rebuild`
also truncate `purge_queue` with the replicated tables.

### Enabling it

Purging is off unless all three of these are set for `replica-sync` (in
`replica/.env`); with any unset nothing is queued either, so a replica without
purge duty accumulates no queue, and the syncer logs at start that purging is off.
Enable it on **one** replica per zone. Every start records whether purging is on
in `purge_enabled` (`/sync-status`) on its first cycle, so switching it off
clears the flag; rows already in the queue stay there, and the syncer logs once
how many (`N accounts are queued for purging but purging is off`) for an
operator to drain by enabling purging again or to delete.

| variable | |
| --- | --- |
| `PURGE_CLOUDFLARE_ZONE` | the zone id of the images domain |
| `PURGE_CLOUDFLARE_TOKEN` | an API token scoped to that zone with the **Zone → Cache Purge** permission only; read from the environment, never logged |
| `PURGE_BASE_URL` | the public base of the redirects, e.g. `https://images.hive.blog` |
| `PURGE_URL_TEMPLATES` | optional: comma-separated URLs with `{base}` and `{name}`; a template containing `/avatar` is purged on an avatar change, one containing `/cover` on a cover change (each has to contain exactly one of them; at most 30) |
| `PURGE_TIME_BUDGET_S` | optional: seconds per drain in which calls are started (default 5); lower it if the zone's purge rate limit is hit |
| `PURGE_CLOUDFLARE_API_URL` | optional: the API base (default `https://api.cloudflare.com/client/v4`; the tests point it at a stub) |

The default URL list is `cf-cache-purger`'s, ten URLs per account (eight on an
avatar change, two on a cover change):

```
{base}/u/{name}/avatar         {base}/webp/u/{name}/avatar
{base}/u/{name}/avatar/small   {base}/webp/u/{name}/avatar/small
{base}/u/{name}/avatar/medium  {base}/webp/u/{name}/avatar/medium
{base}/u/{name}/avatar/large   {base}/webp/u/{name}/avatar/large
{base}/u/{name}/cover          {base}/webp/u/{name}/cover
```

The `/p/<base58(url)>?width=…` objects the redirects point to are keyed by the
image URL itself and need no purge; browsers may keep an old redirect for up
to 10 minutes (`max-age=600`).

### Reading the backlog

`/sync-status` reports `purge_enabled`, `purge_pending`, `purge_oldest_block`
and `purge_last_error`. A `purge_pending` that stays above 0 with a
`purge_last_error` means Cloudflare refuses or is unreachable (a 403 is a
token without Cache Purge on that zone; a 429 the zone's rate limit). The rows
themselves:

```sh
docker compose -f replica/compose.yml --env-file replica/.env exec replica-db \
  psql -U postgres proxy_whitelist -c "table proxy_whitelist_replica.purge_queue"
```

A permanently failing row is retried forever and holds back the rows behind
it; deleting it gives up its purge (its redirects expire within a day).

### Retiring `cf-cache-purger`

`blocktrades/cf-cache-purger` follows the chain separately and purges on every
`account_update`, unordered with the replica's commit. Once purging is enabled
on the img2 replica and a test account's profile change has been seen to queue
and drain (`purge_pending` back to 0, `curl -I
https://images.hive.blog/u/<test>/avatar` redirecting to the new image), stop
`cf-cache-purger` on img2. Its Varnish purge is not replaced: img2 runs no
Varnish.

## Fronting it with haproxy (next step)

The image server's haproxy is to route the imagehoster's `whitelist.apiUrl`
(`http://127.0.0.1`) to this replica's rewriter, with the primary as a `backup`
server, both judged by the agent check above: a down replica degrades to the primary
rather than failing open. That routing, and pointing the imagehoster at it, are
the next issue; this profile only provides the replica.
