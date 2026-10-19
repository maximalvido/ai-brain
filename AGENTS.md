# Agent Instructions

## Package Manager

- Use Cargo with the workspace manifest at `Cargo.toml`.
- Use root `docker-compose.yml` and `deploy/Dockerfile` for PostgreSQL and MCP services.

## File-Scoped Commands

| Task | Command |
|------|---------|
| Format check | `cargo fmt --all -- --check` |
| Lint workspace | `cargo clippy --workspace --all-targets -- -D warnings` |
| Test crate | `cargo test -p <crate-name>` |
| Test workspace | `cargo test --workspace` |
| Compose validation | `BRAIN_API_KEY=test docker compose config` |

## Project Layout

- `apps/brain-server/`: CLI, stdio MCP, and HTTP MCP entry points.
- `crates/`: domain, connector, ingestion, compiler, and storage boundaries.
- `migrations/`: PostgreSQL schema applied on server startup.
- `docker-compose.yml`: PostgreSQL, HTTP MCP, and stdio MCP services.
- `deploy/Dockerfile`: multi-stage image for the server services.
- `docs/`: product, architecture, protocol, and ADR documentation.

## Key Conventions

- Keep source permissions and provenance attached to observations and resources.
- Keep MCP as an adapter over retrieval and do not add provider-specific behavior to the domain model.
- Never commit credentials, company data, source exports, or private URLs.
- Update README and the relevant `docs/` page when changing a user-visible command, service, or protocol.
- Prefer small workspace crates and explicit interfaces over new network services.

## Commit Attribution

AI commits MUST include:

```text
Co-Authored-By: OpenAI Codex <noreply@openai.com>
```
