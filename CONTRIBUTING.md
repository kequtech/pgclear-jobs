# Development and release

The repository uses npm, strict TypeScript, Node.js 24+, and a dedicated PostgreSQL test database.
Keep the configured connection in `.env`; `.env.example` shows the existing local convention.

```sh
npm ci
cp .env.example .env # Only when creating your own local configuration.
npm run check
```

`PGCLEAR_JOBS_TEST_DATABASE_URL` must identify a database ending in `_test`. Integration tests
intentionally remove/reinstall the `pgclear_jobs` schema; use disposable Jobs data. Other application
schemas are left alone. CI runs PostgreSQL 16 and 18. A local pass covers only the local server. Integration tests include a real Next.js fixture: its
Next/React dependencies are installed in a temporary consumer with packed Jobs and pgclear artifacts,
without Tenon. The fixture role needs CREATEDB for its isolated storage.

## Checks

`npm run check` checks formatting/types, builds declarations, tests a packed consumer, and runs unit
and PostgreSQL integration tests. Compile-only cases validate payloads and native/integrated
transaction types. The packed consumer installs into a fresh directory without Tenon and exercises
public imports, declarations, migration resources, and npm's installed command.

Behavioral tests cover rollback, concurrent claims, lost ownership, deadlines, uncooperative
handlers, worker shutdown, schedules, keyed successors, cleanup, and shared-pool pressure. A test-only
TCP relay exercises lost/delayed messages and uncertain commits against real PostgreSQL. It adds no
runtime dependency. Keep tests focused on observable behavior; documentation edits need no wording tests.

```sh
npm run test:unit
npm run test:integration
npm run build
npm pack --dry-run
```

## Load profiles

```sh
npm run test:compile
for sample in 1 2 3; do
  PGCLEAR_JOBS_LOAD_OUTPUT="/tmp/jobs-load-$sample.json" node --env-file-if-exists=.env .test-dist/test/load/queue-load.js
done
PGCLEAR_JOBS_LOAD_INTERVAL_MS=150 npm run test:load
```

The default producer phase is ten seconds, targeting batches of eleven jobs every 50 ms, followed
by backlog drain. Enqueue waits can slow production. Two runners in one Node process each allow four handlers and share a pool with two simulated
application loops. Each profile starts with 1,500 bulk jobs, 20,000 retained and 3,000 expired records;
only generated definitions are cleaned up. Storage must already match the current package.

[November 2 results](test/load/current-2026-11-02.json): Jobs 0.3.1 with its locked pgclear 0.3.3,
Node 24.4.1 and PostgreSQL 16.15 on a local Threadripper/WSL2 host. Medians of three stress runs:

| Profile                  | Completed jobs/s, median (range) | Quiet-job start p95, ms | Application transaction p95, ms |
| ------------------------ | -------------------------------: | ----------------------: | ------------------------------: |
| Light SQL, 4 connections |                    339 (242–346) |                   16.89 |                            7.52 |
| Slow SQL, 2 connections  |                       86 (85–86) |                  858.19 |                           37.40 |

Light/slow profiles use handler SQL waits of 0/8 ms and application SQL waits of 2/12 ms; their
comparison changes work as well as pool size. Quiet-job p95 ranged from 16.80–32.66 ms in light SQL
and 805.21–1,089.70 ms in slow SQL. A single lower-offer-rate diagnostic (150 ms between batches)
had a slow-profile quiet-job p95 of 42.56 ms. All runs completed expected work/retries
without failed jobs or expired attempts. This demonstrates load sensitivity, not a capacity limit.

Throughput includes initial backlog and drain time. Start delay includes enqueue and, for delayed
jobs, intentional timing; retrying jobs contribute multiple starts. Reported p95s are medians of
per-run p95s, not pooled distributions. Setup, cleanup and EXPLAIN observations are outside timed
work; the JSON retains settings, versions, source/driver hashes, per-run metrics and plans.
These synthetic local runs are not CI timing gates or production guarantees. The earlier
[fairness comparison](test/load/claim-fairness-baseline.json) remains historical evidence.

## Releases

Run `npm run check`, inspect `npm pack --dry-run`, and choose the package version before publishing:

```sh
npm publish --access public
```

Corrections require a new version: a published name/version cannot be reused, even after removal.
See [npm publish](https://docs.npmjs.com/cli/v11/commands/npm-publish/). Document API and schema
compatibility changes, and keep saved application migrations immutable.

Validate Tenon's query guards, configuration, migration generation, and lifecycle when changing the
integration; follow the [integration responsibilities](docs/integration.md). Application checks should
exercise idempotency, cancellation, supervision, pool sizing, and rollout with retained, delayed,
and retrying work.

## Documentation and test ownership

README links five consumer guides: reference, storage, reliability, hosting and integration. They and
examples ship in the package. This guide, ARCHITECTURE.md and notes stay repository-only. Check
packed Markdown file/anchor targets and example imports when reorganizing documentation.

Keep native/custom-scope queue semantics, schedules, migration/grant SQL, worker lifecycle and process
hosting tests here. pgclear tests transport and native resource recovery; a query library tests its
own guards, observations and facade expiry. Avoid repeating a lower library's full contract suite
when an integration test can target the boundary.
