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
- an MCP `brain_search` tool for connected AI clients;
- an HTTP MCP endpoint intended to run once on a private company server.

## Deployment model

The intended deployment is one AI Brain server for one company. The server
owns the PostgreSQL database, runs source synchronization, and exposes MCP
over an internal HTTPS URL. Employees connect to that URL from an approved
VPN; their AI clients do not need a local checkout or a local database.

```text
Linear / future sources
          │
          ▼
  AI Brain server + PostgreSQL
          │  HTTPS /mcp
          ▼
       Company VPN
          │
          ▼
  Codex, Claude, or another MCP client
```

For production, place the HTTP endpoint behind a TLS reverse proxy or private
load balancer, restrict network access to the VPN, and use a separate strong
`BRAIN_API_KEY` for the installation. The built-in bearer key is suitable for
the alpha deployment; per-user authentication and auditing are planned.

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

### 2. Start the database and MCP services

```bash
docker compose -f deploy/docker-compose.yml up -d
```

This starts PostgreSQL, the HTTP MCP server, and the stdio MCP service. The
server runs the checked-in migrations when it connects to the database. Set
`BRAIN_API_KEY` in `.env` before starting Compose.

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

### Shared server: HTTP MCP over the company VPN

Compose runs the HTTP endpoint on port `8787` by default. On the AI Brain
server, bind the endpoint to an address reachable by the reverse proxy or
private load balancer:

```bash
docker compose -f deploy/docker-compose.yml up -d http
```

The HTTP MCP endpoint is available at `http://localhost:8787/mcp` locally, or
through the private HTTPS address published by your reverse proxy. Compose
binds it to localhost by default; set `BRAIN_HTTP_BIND` to a private host
interface when a reverse proxy or load balancer runs on another machine. Set
`BRAIN_HTTP_PORT` if the host port 8787 is already in use.

The Compose `mcp` service runs the stdio transport for integrations that can
attach to a container process. For a local client that launches MCP itself,
the Cargo command below remains the simplest option.

Keep `127.0.0.1` when a reverse proxy runs on the same server. If the proxy is
on another private host, bind to the server’s private interface instead. Do
not bind to a public interface unless the host firewall and VPN policy protect
the port.

Publish the endpoint through a private DNS name, for example:

```text
https://ai-brain.example.internal/mcp
```

Every client request must include:

```http
Authorization: Bearer <BRAIN_API_KEY>
```

#### Codex client setup

Run this on the employee’s machine after joining the company VPN:

```bash
codex mcp add ai-brain \
  --url https://ai-brain.example.internal/mcp \
  --bearer-token-env-var BRAIN_API_KEY
```

Set `BRAIN_API_KEY` in the client environment, restart Codex, and verify that
the `brain_search` tool is available. The employee does not need Rust, Cargo,
PostgreSQL, or an AI Brain checkout for this URL-based setup.

#### Other MCP clients

Choose the client’s **remote HTTP/Streamable HTTP MCP** transport and enter:

- URL: `https://ai-brain.example.internal/mcp`
- Authorization: `Bearer <BRAIN_API_KEY>`

Prefer an environment-variable or secret-store reference when the client
supports one. Never put the bearer key in a committed client configuration.

### Local stdio MCP

Stdio remains useful for development, CI, or a single-user installation:

```bash
codex mcp add ai-brain -- cargo run -p brain-server -- mcp
```

This mode requires the repository checkout, `.env`, and PostgreSQL on the same
machine. It is not the normal setup for employees using a shared server.

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
| `BRAIN_HTTP_ADDR` | HTTP bind address on the shared server | `127.0.0.1:8787` |

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
