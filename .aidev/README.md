# Automated contributions

This directory configures the automated development platform (AIDEV) that
contributes to this repository: which branches it uses, how its work reaches
`main`, and how it verifies a change.

## Branches

- `aidev/integration` — the platform's integration branch, cut from `main` and
  protected (no force-push; GitLab never deletes a protected branch on merge).
  Automated work lands here and nowhere else.
- `ai/*` — one branch per task, created and deleted by the platform.
- `session/*` — a session's accumulated work, landed on `aidev/integration` as a batch.

## How work reaches `main`

`aidev handoff` opens a merge request `aidev/integration → main` for a maintainer
to review and merge. Merge it **keeping the source branch** (the project has
`remove_source_branch_after_merge` on; the branch is protected for that reason).

## Verification

`project.yaml` binds the verification slots to `.aidev/run-checks.sh`, which runs
shellcheck, sqlfluff (with the repository's `.sqlfluff`), the pytest suite under
`tests/unit`, and checks that every `.sql` file is applied by
`scripts/install_app.sh`. The `full`, `canary`, `baseline` and `system` slots also
start `.aidev/test-stack.compose.yml` — a HAF testnet with hafah,
reputation_tracker and hivemind installed — install the app into it twice, and run
`.aidev/api_smoke.py`, which serves the installed schema with PostgREST the way
haf_api_node's `proxy-whitelist-postgrest` does and checks `check_url`
(unknown → whitelisted → blacklisted, and the caller's `blacklisted_accounts`),
`url_accounts`, `sync_status` and the root spec. The `full`, `canary` and
`baseline` slots then install the replica (`replica/`) into the stack's plain
Postgres (`replica-db`), bootstrap it with `replica/sync.py` from the primary and
run `.aidev/replica_smoke.py`, which follows the seeded primary cycle by cycle and
compares the two APIs' answers byte for byte.

Not covered by the slots: block processing (the main loop waits for hivemind to
reach the block, and the testnet hivemind has no synced posts) — the production
deploy and CI's image build keep that. The suites run in the image built from
`.aidev/runtime/Dockerfile` (common-ci-configuration's python-test-tools plus
psycopg and HAF's `install_with_app_lock.py`).

Issues: `todo` + `implement` approves an issue for automated work; the label
taxonomy is `.aidev/labels.yaml`.
