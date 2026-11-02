# Runnable examples

Install pgclear-jobs and its pgclear peer in an ESM application, copy the example files outside
node_modules, and set DATABASE_URL. Install storage explicitly before worker startup.

| File               | Purpose                                                           |
| ------------------ | ----------------------------------------------------------------- |
| jobs.ts            | Handler-free greeting specification                               |
| produce.ts         | Submit a greeting through the supplied pool                       |
| worker.ts          | Bind/run a greeting handler with events and explicit error policy |
| database.ts        | Small application-owned connection setup for producer/worker      |
| next-host.mjs      | Supervise production Next and a separate worker process           |
| host-worker.ts     | Native Jobs worker with IPC readiness and ordered pool cleanup    |
| runtime-grants.sql | Storage-owner grants for a pre-existing runtime role              |

For the basic example, keep database.ts/jobs.ts/produce.ts/worker.ts together:

```sh
node --env-file-if-exists=.env worker.ts
# In another process:
node --env-file-if-exists=.env produce.ts
```

For Next, build the application's Next output first, then follow [hosting](../docs/hosting.md).
These are optional application examples, not new library APIs. Replace handlers, process policy and
connection setup with the application's existing choices. Tests compile the TypeScript examples and
exercise packaged imports, native worker lifecycle, grants and real framework signals.
