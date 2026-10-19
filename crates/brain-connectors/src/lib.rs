//! Source connector contracts.
//!
//! Connectors translate source-specific APIs, webhooks, and backfills into
//! common source events. They do not write directly to Brain storage.

use async_trait::async_trait;
use brain_domain::{ConnectorId, DomainError, DomainResult, SourceEvent, SourceSystem};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConnectorCapabilities {
    pub supports_webhooks: bool,
    pub supports_incremental_polling: bool,
    pub supports_historical_backfill: bool,
    pub supports_deletions: bool,
    pub supports_permissions: bool,
    pub supports_reconciliation: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConnectorHealth {
    pub connector_id: ConnectorId,
    pub source_system: SourceSystem,
    pub authenticated: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Cursor(pub String);

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackfillRequest {
    pub resource_types: Vec<String>,
    pub since: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReconcileRequest {
    pub resource_types: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WebhookRequest {
    pub signature: Option<String>,
    pub body: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeBatch {
    pub events: Vec<SourceEvent>,
    pub next_cursor: Option<Cursor>,
}

#[async_trait]
pub trait Connector: Send + Sync {
    fn id(&self) -> ConnectorId;
    fn source_system(&self) -> SourceSystem;
    fn capabilities(&self) -> ConnectorCapabilities;

    async fn validate(&self) -> DomainResult<ConnectorHealth>;
    async fn backfill(&self, request: BackfillRequest) -> DomainResult<ChangeBatch>;
    async fn poll(&self, cursor: Option<Cursor>) -> DomainResult<ChangeBatch>;
    async fn handle_webhook(&self, request: WebhookRequest) -> DomainResult<ChangeBatch>;
    async fn reconcile(&self, request: ReconcileRequest) -> DomainResult<ChangeBatch>;
}

pub fn unsupported(operation: &str) -> DomainError {
    DomainError(format!("connector operation is unsupported: {operation}"))
}
