# PostgreSQL Adapter

Status: In progress

The repository contract and initial migration are ready for a PostgreSQL
implementation. The adapter uses SQLx with the PostgreSQL and Tokio runtime
features.

The adapter is included in the workspace and currently implements observation
persistence. Resource persistence, migration execution, and integration tests
are not part of the current alpha implementation.

## Verification

```bash
cargo fetch
cargo test --workspace
```

The adapter uses SQLx's `PgPool` type and prepared query APIs. See the [SQLx
PgPool documentation](https://docs.rs/sqlx/latest/sqlx/postgres/type.PgPool.html).

The development Compose file reads local settings from `.env` and publishes
PostgreSQL on host port `55432` by default to avoid colliding with other local
PostgreSQL containers. Copy `.env.example` when setting up a new checkout.
