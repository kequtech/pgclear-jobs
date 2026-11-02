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
npm run test:load
PGCLEAR_JOBS_LOAD_DURATION_MS=60000 PGCLEAR_JOBS_LOAD_INTERVAL_MS=150 npm run test:load
```

The default producer phase is ten seconds with 50 ms between batches. Each batch offers eleven
jobs, followed by backlog drain. Two profiles share four or two connections between runners and
simulated application work. Fixtures include ready backlog, retries, delayed work, and retained/
expired history. Set `PGCLEAR_JOBS_LOAD_OUTPUT` to save JSON. Setup/drain/cleanup add time beyond the
producer phase; only generated load definitions are cleaned up afterward.

These are synthetic single-host measurements, not capacity guarantees. The historical fairness
comparison is retained in `test/load/claim-fairness-baseline.json`; it describes an earlier revision,
not current performance. Run the profiles for current measurements and validate real dependencies
in the consuming application.

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
