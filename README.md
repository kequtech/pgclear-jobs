# pgclear-jobs

Durable PostgreSQL jobs using pgclear. Typed producer contracts, transactional submissions, retries,
keyed work and recurring schedules. Node.js 24+, ESM. No ORM dependency.

```sh
npm install @kequtech/pgclear-jobs @kequtech/pgclear
# Set DATABASE_URL, then install new storage explicitly:
npx pgclear-jobs migrate
```

```ts
import { createPool } from '@kequtech/pgclear';
import { defineJob, defineWorker, jobs, runJobs } from '@kequtech/pgclear-jobs';

const connection = process.env.DATABASE_URL;
if (!connection) throw new Error('DATABASE_URL is required.');
const pool = createPool(connection);
const greeting = defineJob<{ name: string }>({ name: 'greet' });
const worker = defineWorker(greeting, {
  handler({ name }) {
    console.log(`Hello, ${name}`);
  },
});
try {
  await jobs(pool).send(greeting, { name: 'Ada' });
  await runJobs(pool, [worker]);
} finally {
  await pool.close();
}
```

Producer contracts can be imported without handlers. Construction/imports create no storage or
workers. Runtime configuration is application-owned; the migration CLI also reads .env. Runners
use the supplied pool and leave it open. `runJobs` runs continuously; shutdown integration belongs
at the application host, as shown in [Hosting](docs/hosting.md#startup-and-shutdown).

| Task                                                                  | Documentation                      |
| --------------------------------------------------------------------- | ---------------------------------- |
| Install storage, apply an upgrade or grant runtime access             | [Storage](docs/storage.md)         |
| Look up definitions, record methods, schedules and runner options     | [Reference](docs/reference.md)     |
| Choose transaction boundaries and understand failure/retry guarantees | [Reliability](docs/reliability.md) |
| Host a worker, observe readiness or run it beside Next.js             | [Hosting](docs/hosting.md)         |
| Bind another pgclear-based query API                                  | [Integration](docs/integration.md) |

Jobs 0.3.0 uses storage version 3 and supports cancelling running jobs through the same cancel(id)
method. Existing version-2 storage needs jobsCancellationUpgrade in a new application migration;
version-1 storage first needs jobsUpgrade. Stop old producers/workers before migrating. Never reset
queued work to upgrade. See [Storage](docs/storage.md).
The guides, SQL resources and examples ship with the package; read the installed version's docs.

pgclear owns connection/transaction/cancellation behavior. Jobs owns claims, retries, scheduling,
retention and worker lifecycle. Applications own business results, external effects and process
policy. [Tenon](https://github.com/kequtech/tenon-orm) optionally supplies ORM transactions through
the same integration factory; it is not required.

[Changelog](CHANGELOG.md) · [Examples](examples/README.md) ·
[Contributing](https://github.com/kequtech/pgclear-jobs/blob/main/CONTRIBUTING.md) ·
[Architecture](https://github.com/kequtech/pgclear-jobs/blob/main/ARCHITECTURE.md)
