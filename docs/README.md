# pgclear-jobs documentation

These documents describe the implemented library.

- [Schema migrations](migrations.md): the install/remove command and one up/down SQL pair for application migration files.
- [Integration API](integration.md): the typed transaction/executor boundary for another pgclear-backed query library.

- [Implementation plan](implementation-plan.md): target package boundaries, opt-in ORM integration,
  generated SQL migrations and the staged implementation sequence. Proposed APIs are marked there.

- [Quick start](../README.md): definitions, transactions, runners and common workflows.
- [API](api.md): public methods, options, return values and errors.
- [Storage and reliability](storage-and-reliability.md): claims, retries, schedules and failure boundaries.
- [Architecture](architecture.md): responsibilities and the relationship to pg-boss.

Outstanding work and adoption checks live in [todo.md](../todo.md).
