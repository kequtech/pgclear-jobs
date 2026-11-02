# Hosting workers

[Overview](../README.md) · [Runner reference](reference.md#runner) · [Reliability](reliability.md)

## Startup and shutdown

The basic worker needs only `await runJobs(pool, definitions)`. For graceful shutdown, pass the
application host's existing shutdown signal. The engine stops claiming work, drains active handlers
and aborts their scopes if the grace period expires. Handlers receive their own signal automatically.
One host signal can serve multiple runners; no controller is needed per runner or handler.

When the host also needs readiness:

```ts
import { startJobs } from '@kequtech/pgclear-jobs';

// shutdownSignal comes from the application's host lifecycle.
const worker = startJobs(pool, definitions, { signal: shutdownSignal });
void worker.done.catch(stopApplication); // Application failure policy; attach immediately.
await worker.ready;
// Start accepting requests; the host aborts shutdownSignal during shutdown.
await worker.done;
await pool.close();
```

startJobs returns {ready,done} using the same engine/options as runJobs. runJobs's promise remains
the full worker lifetime. Ready resolves after storage validation and successful onReady, before
claims, maintenance or listener startup. It confirms neither listener connectivity nor ongoing health.
Startup failures reject both promises with the original error. Shutdown before readiness rejects
ready with AbortError and normally resolves done; later failure rejects done without changing ready.
Both promises are observed internally, but the application must still supervise failures.

A pending onReady callback is interrupted by shutdown; its later rejection is observed. Its own
side effects should honor application cancellation. Do not wait for a job to finish inside onReady.

The application owns process signals and restart policy. In a standalone Node process, a single
AbortController can translate SIGINT/SIGTERM into the host shutdown signal; the
[worker example](../examples/worker.ts) shows this optional wiring. Framework hosts can supply their
existing lifecycle signal instead. Await done before closing the pool. Separate runs have independent
lifetimes. Pass handler signals to dependencies and preserve cleanup failures alongside original errors.

## Framework-owned exit

A framework may exit on SIGTERM without awaiting another asynchronous signal callback. The
[Next host example](../examples/next-host.mjs) supervises framework and worker child processes.
It starts Next after worker readiness, stops both on signal/child failure and awaits cleanup.
Next's own SIGTERM exit code 143 is accepted only during requested shutdown. The fixed 40-second
host grace covers the default 30-second worker grace plus pool cleanup; adapt it to your host.

Copy examples into an application with a production Next build:

```sh
cp node_modules/@kequtech/pgclear-jobs/examples/next-host.mjs ./next-host.mjs
cp node_modules/@kequtech/pgclear-jobs/examples/host-worker.ts ./worker.ts
node --env-file-if-exists=.env next-host.mjs ./worker.ts
```

The [worker entry](../examples/host-worker.ts) uses native pgclear/Jobs and sends {type:'ready'}
over IPC after validation. Replace its bindings with application workers. It owns its pool, handles
signals/parent disconnect and retains original/cleanup errors. It can also run standalone.
The parent owns no database resources. PORT is forwarded to Next. This is optional application
hosting code, not a Jobs process-manager API or a Next runtime dependency.

The real Next fixture tests startup, SIGINT/SIGTERM, worker failure and framework failure against
a packed standalone Jobs installation without Tenon. [Runnable examples](../examples/README.md)
also cover a producer and an ordinary worker.

## Host startup order

Await application startup work, such as reconciliation, before starting the runner. Await ready before
accepting requests, and observe done for the entire host lifetime. Ready confirms storage validation
and the readiness hook; it does not confirm notification connectivity or ongoing health.

On shutdown, stop incoming requests/producers, signal the runner, await its completion and other
application-owned tasks, then close their pools/listeners. A cancelled readiness hook may still be
running JavaScript; the host must await its own startup task before disposing resources it uses.
Keep the host shutdown budget longer than the worker grace plus resource cleanup. Preserve both
work and cleanup errors; the shipped process examples do so without installing process handlers in
library code. Set loaders/environment in the application worker entry as its existing tooling needs.

## Observe and escalate

onEvent is best-effort local observation. Observer throws/rejections warn without affecting outcomes;
callbacks are not awaited. Keep payloads/raw errors out of logs unless deliberately redacted. Events
are not a durable product event stream or a substitute for stored queue/application state.

onHandlerError chooses continue or stop after an escaping handler exception and confirmed managed
rollback. Continue retains normal saved retry/JobError policy. Stop rejects the runner with the
original error before retry bookkeeping, leaving lease recovery rather than a durable pause.
Managed opening/commit/cleanup failures bypass this hook and reject the runner. Independent
application-transaction errors remain handler failures unless the application escalates them.
The [reference](reference.md#observation-and-error-policy) specifies hook fields and interruption rules.
