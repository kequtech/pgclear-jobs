# Documentation

The Jobs API is pgclear-only. Tenon ORM implements its facade over the same engine.

- [Quick start](../README.md): install, submit, and run work.
- [API reference](api.md): definitions, record operations, schedules, errors, and runner options.
- [Reliability and operations](storage-and-reliability.md): guarantees, failure boundaries, and shutdown.
- [Schema setup](migrations.md): installation, explicit upgrade SQL, runtime grants and intentional removal.
- [Integration contract](integration.md): bind a pgclear-backed query library to the existing engine.
- [Architecture](architecture.md): ownership and source responsibilities.
- [Development and release](development.md): tests, load profiles, packaging, and releases.

The [Tenon integration](implementation-plan.md) describes ownership and acceptance checks. Remaining adoption checks are in [todo.md](../todo.md).
