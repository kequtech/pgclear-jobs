# Tenon Jobs design

Design record, 31 October 2026.

Tenon Jobs is a planned TypeScript library for durable PostgreSQL jobs. It is a sister library to
Tenon ORM, built directly on pgclear, with an optional Tenon integration. The intended package name
is `@kequtech/tenon-jobs`. The current directory name, `tenon-work`, is the earlier working title.
Package-name availability and publication have not been established.

This directory preserves the planning conversation and its supplied handoff. Development began
after the subsequent project setup request. The [implementation contract](implementation.md)
and [root README](../README.md) describe the working experimental subset; the design documents
below preserve proposals and open questions. No package publication is authorized.

## Read the design

- [Public API and application workflow](api.md): definitions, submission, lookup, modification,
  and execution.
- [Architecture and execution](architecture.md): package boundaries, transaction ownership,
  connection ownership, and runners.
- [Storage and reliability](storage-and-reliability.md): shared storage, claims, recovery,
  duplicate prevention, and concurrent mutations.
- [Scope, decisions, and remaining questions](decisions.md): what is settled, what is proposed,
  and what needs further design.

## Governing principles

- Keep the public model small: a definition, a persisted job, and an explicit runner.
- Define the name, typed handler, and execution policy together once.
- Require a database scope for record operations: `jobs(dbOrTx)`.
- Insert jobs atomically with application writes when a transaction is supplied.
- Execute committed jobs independently of the submitting transaction or request.
- Support running jobs alongside the application or in separate processes.
- Let each runner select the job definitions it processes.
- Use one shared set of queue tables per database across all processes and job types.
- Preserve at-least-once execution semantics and make failures inspectable.
- Add useful capabilities through a coherent model, without a subsystem for every feature.

Tenon Jobs is intended to be independently useful. Its family name does not make Tenon ORM a
requirement for standalone producers or runners.

## Status of examples

The conversation converged on this public shape:

```ts
const reminder = defineJob({
  name: 'reminder',
  async handler(payload: Payload) {
    /* ... */
  },
});
const job = await jobs(tx).send(reminder, payload);
await runJobs(pool, [reminder]);
```

These are intended interfaces, not existing exports. Detailed option names, query filters,
mutation errors, shutdown behavior, and storage layout remain proposals where identified below.
Later accepted decisions take precedence over earlier suggestions preserved in the handoff.
