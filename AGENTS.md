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
- `docs/`: public documentation for the current published version.
- `docs/private/`: local, ignored roadmaps, drafts, designs, evaluations, ADRs,
  and other working material that must not be published.

## Documentation Workflow

- Keep `docs/` limited to documentation for the current published version.
- Keep roadmaps, unreleased designs, meeting notes, evaluations, and ADRs in
  the ignored `docs/private/` or `docs/drafts/` directories.
- When adding or changing a user-visible command, service, protocol, or
  connector, update all of the following as applicable:
  1. the relevant page in `docs/`;
  2. the public index at `docs/README.md`;
  3. the root `README.md` when setup, configuration, or commands change.
- Mark incomplete or pilot functionality clearly as `Experimental` in the
  relevant public docs and explain its scope and limitations.
- Do not link to ignored files from public documentation. Check for stale
  links and private filenames before committing:

```bash
git diff --check
rg -n "docs/(private|drafts|adr)/|docs/codex-yolo|docs/07-roadmap|docs/08-implementation|codex-yolo" README.md CONTRIBUTING.md docs || true
```

- For documentation-only changes, run `git diff --check`; for changes that
  describe code or deployment behavior, also run the relevant Cargo and Compose
  checks from the command table above.

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
