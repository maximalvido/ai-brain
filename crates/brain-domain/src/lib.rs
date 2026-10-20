//! Core domain contracts shared by AI Brain modules.
//!
//! This crate intentionally has no storage, network, or model-provider
//! dependencies. It defines the vocabulary exchanged between those modules.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ConnectorId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ObservationId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ResourceId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PrincipalId(pub String);

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SourceObjectId {
    pub system: SourceSystem,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SourceSystem {
    Linear,
    Slack,
    Claude,
    Other(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ObservationOperation {
    Upsert,
    Delete,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourcePermission {
    pub resource_key: String,
    pub principal_key: String,
    pub can_read: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceEvent {
    pub connector_id: ConnectorId,
    pub source_object_id: SourceObjectId,
    pub resource_type: String,
    pub operation: ObservationOperation,
    pub source_version: Option<String>,
    pub source_timestamp: Option<String>,
    pub observed_at: String,
    pub payload: Option<String>,
    pub permissions: Vec<SourcePermission>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Observation {
    pub id: ObservationId,
    pub event: SourceEvent,
    pub payload_reference: Option<String>,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Resource {
    pub id: ResourceId,
    pub resource_type: String,
    pub source_object: SourceObjectId,
    pub data: serde_json::Value,
    pub first_observed_at: String,
    pub last_observed_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainError(pub String);

impl std::fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DomainError {}

pub type DomainResult<T> = std::result::Result<T, DomainError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_event_preserves_source_identity() {
        let event = SourceEvent {
            connector_id: ConnectorId("linear-main".into()),
            source_object_id: SourceObjectId {
                system: SourceSystem::Linear,
                value: "issue-123".into(),
            },
            resource_type: "issue".into(),
            operation: ObservationOperation::Upsert,
            source_version: Some("42".into()),
            source_timestamp: None,
            observed_at: "2026-10-17T00:00:00Z".into(),
            payload: Some("{}".into()),
            permissions: vec![],
        };

        assert_eq!(event.source_object_id.value, "issue-123");
        assert_eq!(event.source_object_id.system, SourceSystem::Linear);
    }
}
