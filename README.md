# AI Brain

AI Brain is a self-hosted, open-source knowledge layer for company data. It
ingests information from business systems, preserves provenance and source
permissions, and makes that knowledge searchable from compatible AI clients
through the Model Context Protocol (MCP).

The first release is read-oriented: it helps an assistant find grounded
company knowledge and cite where it came from. It is not an autonomous
workflow engine and does not replace the systems where source data is managed.

## Status

AI Brain is early alpha software (`v0.1.0-alpha.1`). The core workspace,
PostgreSQL adapter, Linear connector, CLI search command, and local MCP server
are under active development. APIs and database schemas may change between
alpha releases.

## What it provides

- Connectors for company systems, starting with Linear, with Slack planned;
- replayable ingestion of raw source observations;
- canonical resources and provenance-aware compilation;
- PostgreSQL-backed full-text retrieval;
- source references and freshness metadata;
- an MCP `brain_search` tool for local AI clients;
- a small HTTP MCP endpoint for trusted remote clients.

## Recommended repository name

`sourcebound` is the strongest public-facing name: it communicates that answers
are grounded in source material and leaves room for connectors beyond a single
AI provider. Other options are `evidence-brain`, `company-context`, and
`grounded-knowledge`.

The working product name remains **AI Brain** until a repository rename is
chosen.

## Quick start

### Prerequisites

- Rust stable with Cargo;
- Docker or another local PostgreSQL 16 installation;
- a Linear API key if you want to run the Linear connector;
- an MCP-compatible client such as Codex, Claude Desktop, or a custom client.

### 1. Configure the environment

```bash
cp .env.example .env
```

Review `.env` and set at least a private `BRAIN_API_KEY`. Keep `.env` out of
version control. The default configuration uses PostgreSQL on port `55432` so
it does not collide with a typical local PostgreSQL installation.

### 2. Start PostgreSQL

```bash
docker compose -f deploy/docker-compose.yml up -d
```

The server runs the checked-in migrations when it connects to the database.

### 3. Build and test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 4. Ingest source data

Set `LINEAR_API_KEY` in `.env`, then validate the connector and synchronize:

```bash
cargo run -p brain-server -- validate-linear
cargo run -p brain-server -- sync
```

### 5. Search locally

```bash
cargo run -p brain-server -- search "billing"
```

The search command is a development probe over the canonical resource store.

## Using AI Brain with MCP

### Local stdio MCP

The server exposes a read-only `brain_search` tool over standard input/output.
For Codex, register it from the repository directory:

```bash
codex mcp add ai-brain -- cargo run -p brain-server -- mcp
```

Then restart the client and ask questions such as:

> Search the company knowledge base for marketplace requirements and cite the source links.

The MCP process needs access to the repository, `.env`, and PostgreSQL. If a
client cannot set a working directory, launch it from this repository or use
an absolute Cargo manifest path in that client’s configuration.

### HTTP MCP

Start the HTTP endpoint:

```bash
cargo run -p brain-server -- serve-http
```

It listens at `http://127.0.0.1:8787/mcp` by default. Requests must include:

```http
Authorization: Bearer <BRAIN_API_KEY>
```

Do not expose this endpoint to an untrusted network without stronger
authentication, TLS, and deployment-level access controls.

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `DATABASE_URL` | PostgreSQL connection string | Local URL in `.env.example` |
| `POSTGRES_DB` | Compose database name | `ai_brain` |
| `POSTGRES_USER` | Compose database user | `ai_brain` |
| `POSTGRES_PASSWORD` | Compose database password | `ai_brain_local` |
| `POSTGRES_PORT` | Host port mapped to PostgreSQL | `55432` |
| `LINEAR_API_KEY` | Read-only Linear connector credential | unset |
| `BRAIN_API_KEY` | Installation key for MCP and HTTP access | unset |
| `BRAIN_HTTP_ADDR` | HTTP bind address | `127.0.0.1:8787` |

## Repository layout

```text
apps/brain-server/             CLI, stdio MCP, and HTTP MCP entry points
crates/brain-domain/           Domain types and contracts
crates/brain-connectors/       Connector abstractions
crates/connector-linear/       Linear source connector
crates/brain-ingestion/        Observation ingestion
crates/brain-compiler/         Canonical resource compilation
crates/brain-storage/          Storage interfaces and query contracts
crates/brain-storage-postgres/ PostgreSQL implementation
deploy/                         Local infrastructure
docs/                           Product, architecture, and ADR documentation
migrations/                     SQL schema migrations
```

## Documentation

- [Contributing](CONTRIBUTING.md)
- [Product scope](docs/01-product-scope.md)
- [Architecture](docs/02-architecture.md)
- [Knowledge model](docs/03-knowledge-model.md)
- [Connectors and ingestion](docs/04-connectors-and-ingestion.md)
- [Authentication and permissions](docs/05-authentication-and-permissions.md)
- [Retrieval and MCP](docs/06-retrieval-and-mcp.md)
- [Roadmap](docs/07-roadmap.md)
- [Implementation design](docs/08-implementation-design.md)
- [Evaluation](docs/09-evaluation.md)
- [PostgreSQL adapter](docs/10-postgres-adapter.md)
- [Architecture decision records](docs/adr/)

## Security

Never commit API keys, database credentials, exported company data, or source
snapshots. Report security issues privately to the maintainers rather than
opening a public issue with sensitive details. The installation-level API key
is suitable for a trusted private alpha deployment; per-user identity and
source-level authorization are still evolving.

## License

AI Brain is intended to be released under the [Apache License 2.0](LICENSE).
