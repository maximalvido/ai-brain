# Retrieval and MCP

Status: Implemented in the alpha server for stdio and HTTP transports

## Purpose

AI Brain exposes knowledge retrieval to MCP-compatible clients. It returns compact, structured, permission-filtered evidence. The client is responsible for turning that evidence into the final conversational response.

## Deployment topology

The primary deployment is one private AI Brain installation per company. The
installation runs the source connectors, PostgreSQL, and the MCP HTTP endpoint
on a server reachable through the company VPN. Employee AI clients connect to a
private HTTPS URL such as `https://ai-brain.example.internal/mcp`.

The HTTP endpoint authenticates requests with the installation-level
`BRAIN_API_KEY` bearer token. Production deployments should add TLS, private
DNS, VPN/network policy, secret management, rate limiting, and operational
logging at the deployment boundary. The current alpha does not yet provide
per-user identity or per-user API keys.

Stdio MCP is retained for local development and single-user deployments. It
should not be required on employee machines in the shared-server model.

## Compose deployment

The repository includes a multi-stage `deploy/Dockerfile` and root-level
`docker-compose.yml`. The Compose services are:

- `postgres`: PostgreSQL 16 with the persistent `ai-brain-postgres` volume;
- `http`: HTTP MCP transport at `/mcp`, published on host port `8787` by
  default and bound to `127.0.0.1`;
- `mcp`: stdio MCP transport for an integration that can attach to a container
  process.

Start the full local stack with:

```bash
cp .env.example .env
# Set BRAIN_API_KEY in .env.
docker compose up -d
```

Set `BRAIN_HTTP_BIND` and `BRAIN_HTTP_PORT` when the HTTP service must be
reachable on a private host interface or a different host port. The Compose
HTTP service sets `BRAIN_HTTP_ADDR=0.0.0.0:8787` inside the container; host
interface exposure is controlled by the port mapping.

## Query lifecycle

```text
MCP request
  → authenticate principal
  → interpret natural-language query
  → create structured query
  → select retrieval strategy
  → retrieve candidates
  → apply permissions
  → rank and select evidence
  → return results and citations
```

## Query interpretation

The interpreter may use deterministic parsing, model assistance, or both. Model output must be schema-validated and must not directly authorize access or execute source actions.

Example structured query:

```json
{
  "text": "What is blocking the mobile release?",
  "intent": "project_status",
  "entities": ["mobile release"],
  "filters": {"state": "active"},
  "time_range": "current",
  "needs": ["issues", "blockers", "recent_discussions"]
}
```

## Retrieval strategies

The planner can combine:

- exact and full-text search;
- semantic search;
- structured filters;
- entity lookup;
- relationship traversal;
- temporal filtering.

The first implementation should use hybrid full-text and semantic retrieval, with structured filters where the query identifies known entities or fields.

## MCP tools for v0.1

### `brain_search`

The alpha server exposes `brain_search`, which searches authorized resources
and returns compact source-backed hits.

Inputs:

- `query` text;
- optional `resource_type`;
- optional `limit` from 1 to 100.

Returns ranked results with resource IDs, source-system identifiers, source URLs
when available, summaries, timestamps, and relevance scores. `get_resource`,
`get_evidence`, and `find_related` remain planned capabilities rather than
implemented alpha tools.

## Result contract

Every result should include, where available:

```text
- result_id
- result_type
- title or label
- compact content or structured value
- relevance score
- source system
- source object ID
- source URL
- source timestamps
- observed timestamp
- freshness state
- confidence
- provenance references
```

The API should distinguish:

- no matching knowledge;
- matching knowledge that is stale;
- matching knowledge that is contradictory;
- matching knowledge that cannot be returned because of authorization.

Authorization failures must not reveal the existence of hidden resources.

## MCP independence

The internal retrieval API is the primary contract. MCP is an adapter over that contract. Provider-specific behavior for ChatGPT, Claude, Gemini, or other clients must not leak into the knowledge model or retrieval engine.

## Future capabilities

- subscriptions to knowledge changes;
- answer synthesis inside AI Brain;
- saved searches;
- query explanations;
- task-specific context assembly.

These are not required for v0.1.
