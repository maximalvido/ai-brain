# Product Scope

Status: Proposed

## Problem

Company information is fragmented across systems. People and AI agents spend too much time searching, joining context manually, and deciding which source to trust.

AI Brain provides a shared knowledge layer that makes company information retrievable without flattening source authority, permissions, provenance, or freshness.

## Target users

### Primary users

All employees who need to understand company information, projects, decisions, ownership, and current work.

### System administrators

Engineering or platform teams configure connectors, identities, permissions, indexing, and operational settings.

### Extenders

Connector developers, platform engineers, and AI application developers who want to add sources or consume the retrieval API.

## Initial use cases

- Find the current status of a project.
- Identify blockers, owners, and related work.
- Find the discussion or decision behind a project change.
- Answer questions that require Linear and Slack together.
- Retrieve recent company or project information with citations.
- Expose company knowledge to MCP-compatible AI clients.

## Product principles

1. Source data and derived knowledge are different things.
2. Retrieval must respect source permissions.
3. Important knowledge must remain traceable to evidence.
4. Deterministic processing is preferred where it is sufficient.
5. Model-generated outputs require schemas, validation, and evaluation.
6. Every derived representation must be rebuildable.
7. The first deployment should be simple to run locally and self-host.
8. Extensions should be possible without weakening the core knowledge model.

## Definition of success for v0.1-alpha

An administrator can configure Linear and Slack, synchronize data, connect an MCP client, and ask realistic company questions. The system returns relevant, permission-filtered evidence with source references and freshness information.

## Non-goals for v0.1

- Writing actions back to Slack or Linear.
- Autonomous workflows.
- Cloud multi-tenancy.
- A dedicated graph database.
- Perfect company-wide ontology discovery.
- Fine-tuning models on company data.
- Supporting every MCP client with provider-specific behavior.
- Replacing the source systems as systems of record.

## Open decisions

- Exact local authentication flow.
- Initial embedding provider and whether local embeddings are required.
- Exact PostgreSQL and object-storage packaging.
- Whether v0.1 supports only source-native permissions or also Brain-managed overrides.
