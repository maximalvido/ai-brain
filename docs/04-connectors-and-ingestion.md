# Connectors and Ingestion

Status: Proposed

## Goals

Connectors hide source-specific APIs while exposing a consistent, replayable ingestion contract. Webhooks are preferred when available, but polling and backfills must remain first-class capabilities.

The current Linear connector uses incremental polling. The `linear-sync`
Compose service performs an initial backfill and stores the latest Linear
`updatedAt` value in `connector_cursors`; later runs request only resources
updated at or after that cursor, relying on idempotent observation writes to
make the small overlap safe. The interval is configured with
`LINEAR_SYNC_INTERVAL_SECONDS` and defaults to five minutes.

## Connector responsibilities

A connector owns:

- source authentication;
- source API calls;
- webhook signature validation;
- source pagination and rate limits;
- source object normalization;
- source permission discovery;
- source cursor or checkpoint format;
- source-specific deletion and reconciliation behavior.

The ingestion runtime owns retries, idempotency, durable observations, job state, replay, and dead-letter handling.

## Capability declaration

```text
ConnectorCapabilities
- supports_webhooks
- supports_incremental_polling
- supports_historical_backfill
- supports_deletions
- supports_permissions
- supports_reconciliation
```

## Common source event

Webhook delivery, polling, and backfill should converge on one internal representation:

```text
SourceEvent
- connector_id
- source_system
- source_object_id
- operation: created | updated | deleted
- source_version
- source_timestamp
- observed_timestamp
- payload
- source_permissions
```

## Ingestion flow

```text
webhook ───────┐
poller ────────┼──→ SourceEvent → idempotency check
backfill ──────┘                         ↓
                              durable observation
                                         ↓
                                  compiler queue
```

The event must be durably recorded before downstream compilation is considered reliable.

## Idempotency

The preferred event identity is a tuple of connector, source object ID, source operation, and source version. When a source does not provide versions, use a stable payload hash plus source timestamps and retain a reconciliation path.

Duplicate delivery must be safe. Reprocessing the same observation must not create duplicate resources, assertions, relationships, or index entries.

## Ordering and deletion

Events may arrive out of order. The compiler should compare source versions or timestamps and retain observation history rather than assuming delivery order is authoritative.

Deletes must be represented as observations, not silently removed. Permission changes should trigger re-indexing or invalidation of affected derived knowledge.

## Polling and webhooks

Webhooks reduce latency but can be duplicated, delayed, or lost. Connectors should use webhooks for responsiveness and periodic polling or reconciliation for recovery when the source supports it.

## Initial connector scope

Linear and Slack are read-only connectors for v0.1. Their first responsibility is reliable synchronization and permission capture. Write actions are out of scope.
