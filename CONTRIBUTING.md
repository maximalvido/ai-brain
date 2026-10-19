# Contributing to AI Brain

Thank you for helping build an open-source, source-grounded knowledge system.
Contributions are welcome in code, connectors, documentation, tests, examples,
and architecture discussions.

## Before you start

1. Read the [README](README.md) and the relevant documents in `docs/`.
2. Check existing issues and pull requests before starting substantial work.
3. For a new connector, storage boundary, authorization behavior, or public
   protocol change, open a design issue first.
4. Never include real company data, credentials, private source URLs, or raw
   exports in a commit, fixture, screenshot, or issue.

## Local setup

```bash
git clone <repository-url>
cd ai-brain
cp .env.example .env
docker compose -f deploy/docker-compose.yml up -d
cargo build --workspace
```

Set `BRAIN_API_KEY` in `.env`. Set `LINEAR_API_KEY` only when working on the
Linear connector or running a real synchronization.

## Development workflow

Keep changes focused and explain the user or system behavior they change.
Prefer small, reviewable commits. Update the relevant documentation and an
architecture decision record when a change affects a contract or boundary.

Run the local checks before opening a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For database-backed work, use the Compose PostgreSQL service and a disposable
local database. Tests must not depend on a developer’s existing database
contents.

## Design principles

- Preserve raw observations so ingestion can be replayed.
- Keep source provenance attached to derived knowledge.
- Enforce authorization before knowledge leaves the system.
- Keep connector-specific behavior behind connector boundaries.
- Treat model-generated text as untrusted and non-authoritative.
- Prefer deterministic, inspectable behavior over opaque retrieval magic.
- Keep the first release read-oriented; actions require explicit design and
  authorization review.

## Adding a connector

Start with the connector contract in `crates/brain-connectors/`. A connector
should translate source records into the shared observation model without
silently discarding source identifiers, timestamps, links, or access metadata.

Include:

- configuration and credential documentation;
- fixture-based tests with sanitized data;
- idempotency behavior for repeated synchronization;
- handling for deleted, changed, and inaccessible source records;
- source links and provenance in resulting observations;
- a short design note if the source has unusual permission semantics.

## Pull requests

A good pull request includes:

- a concise problem statement;
- the approach and important tradeoffs;
- tests or an explanation of why tests are not applicable;
- documentation updates for user-visible behavior;
- migration and rollback considerations, when relevant;
- security and permission considerations, when relevant.

Pull requests should pass formatting, Clippy, and workspace tests. Reviewers
may request changes when a patch weakens provenance, authorization,
replayability, or the separation between source systems and the knowledge
layer.

## Commit messages

Use short imperative subjects, for example:

```text
Add Linear observation deduplication
Document local HTTP MCP setup
Test permission filtering for derived resources
```

## Reporting security issues

Do not publish credentials, private data, or an exploitable vulnerability in a
public issue. Contact the maintainers privately with reproduction details and
the affected commit or release.

## License

By contributing, you agree that your contribution may be distributed under the
project’s Apache License 2.0 license.
