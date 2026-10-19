# PostgreSQL Adapter

Status: In progress

The repository contract and initial migration are ready for a PostgreSQL implementation. The adapter will use SQLx with the PostgreSQL and Tokio runtime features.

The adapter is now included in the workspace. It currently implements observation persistence; resource persistence, migration execution, and integration tests follow next.

## Enablement command

To verify dependencies and the workspace:

```bash
cargo fetch
cargo test --workspace
```

The adapter uses SQLx's `PgPool` type and prepared query APIs. See the [SQLx PgPool documentation](https://docs.rs/sqlx/latest/sqlx/postgres/type.PgPool.html).

## Planned responsibilities

- connect and health-check PostgreSQL;
- run migrations;
- append observations idempotently;
- list observations for replay;
- persist canonical resources;
- enforce database constraints;
- expose transaction boundaries to ingestion.

The in-memory repository remains the unit-test implementation. PostgreSQL integration tests should run against a disposable local database and remain separate from default unit tests.

The development Compose file reads local settings from `.env` and publishes PostgreSQL on host port `55432` by default to avoid colliding with other local PostgreSQL containers. Copy `.env.example` when setting up a new checkout.
