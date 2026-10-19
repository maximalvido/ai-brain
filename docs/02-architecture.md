# Architecture

Status: Proposed

## Boundary

AI Brain owns ingestion, canonicalization, knowledge compilation, indexing, provenance, freshness, authorization-aware retrieval, and MCP exposure.

Source systems remain authoritative for original records and source permissions. The consuming AI client remains responsible for conversation and final response wording.

## Conceptual architecture

```text
Linear / Slack / future sources
              |
       connector adapters
              |
        source events
              |
     raw observations archive
              |
       knowledge compiler
              |
 canonical resources, assertions,
 relations, provenance, permissions
              |
       search and vector indexes
              |
      permission-aware retrieval
              |
          MCP server
              |
        AI client or agent
```

## Initial deployment shape

Use a modular monolith with background workers. Keep module boundaries explicit without requiring each module to be a network service.

```text
AI Brain application
├── administration
├── authentication and authorization
├── connector SDK and runtime
├── ingestion and replay
├── canonical knowledge model
├── compiler stages
├── indexing
├── retrieval and query planning
└── MCP adapter
```

## Initial storage capabilities

- Relational database for canonical records, metadata, relationships, permissions, and job state.
- Raw object storage for source payloads and large documents.
- Full-text index for exact terms, identifiers, and names.
- Vector index for semantic retrieval.
- Relationship tables for initial graph-like traversal.

The first implementation should use one reference relational backend with full-text and vector capabilities where practical. Backend abstractions should be introduced at meaningful boundaries, not as a promise to support every database from the beginning.

## Knowledge lifecycle

```text
source change
  → durable observation
  → normalization
  → deduplication
  → permission update
  → entity linking
  → assertions and relations
  → search projections
```

Each stage records its input, output, version, status, and errors. Derived data can therefore be rebuilt without contacting the source again when raw observations are available.

## Query lifecycle

```text
MCP request
  → authenticate principal
  → interpret query
  → build retrieval plan
  → retrieve candidates
  → apply permissions
  → rank and select evidence
  → return structured results and citations
```

The query interpreter may use a model for ambiguous natural language, but the output must be schema-validated. The retrieval planner and authorization checks remain observable and deterministic.

## Deferred complexity

- Dedicated graph database.
- Distributed event streaming.
- Independent microservices.
- Hierarchical company-wide summaries.
- Autonomous compiler agents.
- Cloud tenant management.
