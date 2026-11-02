# Native pgclear producer and worker

Set `DATABASE_URL` to a local example database in the environment or `.env`. From the checkout:

```sh
npm ci
npm run jobs:migrate
npm run example:worker
```

In another, submit a job:

```sh
npm run example:produce -- Ada
```

`jobs.ts` is the shared handler-free contract; `worker.ts` binds the handler. The producer prints
the UUID; pass it as the second argument to reuse a retained submission. Pass `fail` as the name
to demonstrate permanent failure. SIGINT/SIGTERM stops the worker before its pool closes.
For callback guarantees, see [observation and error policy](../docs/api.md#observation-and-error-policy).

The scripts use Node.js 24's TypeScript support. For an installed package, copy the example into
your ESM application first because Node.js does not strip TypeScript under `node_modules`:

```sh
cp -R node_modules/@kequtech/pgclear-jobs/examples ./jobs-example
node --env-file-if-exists=.env jobs-example/worker.ts
# In another terminal:
node --env-file-if-exists=.env jobs-example/produce.ts Ada
```

The copied files use the application's installed Jobs and pgclear dependencies.
