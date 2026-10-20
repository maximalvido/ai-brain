//! Importer for organization-shared Claude artifact exports.
//!
//! Anthropic does not publish a stable export archive schema, so this importer
//! accepts JSON records only when their organization visibility is explicit.
//! Unknown or private records are ignored rather than guessed into the index.

use async_trait::async_trait;
use brain_connectors::{
    BackfillRequest, ChangeBatch, Connector, ConnectorCapabilities, ConnectorHealth, Cursor,
    ReconcileRequest, WebhookRequest, unsupported,
};
use brain_domain::{
    ConnectorId, DomainError, DomainResult, ObservationOperation, SourceEvent, SourceObjectId,
    SourceSystem,
};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use zip::ZipArchive;

const CONNECTOR_ID: &str = "claude-artifacts";

pub struct ClaudeArtifactConnector {
    path: PathBuf,
    id: ConnectorId,
}

impl ClaudeArtifactConnector {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            id: ConnectorId(CONNECTOR_ID.into()),
        }
    }

    pub fn import_path(&self) -> &Path {
        &self.path
    }

    pub fn parse_json(value: &Value) -> DomainResult<ChangeBatch> {
        let mut events = Vec::new();
        collect_events(value, &mut events)?;
        events.sort_by(|left, right| {
            left.source_object_id
                .value
                .cmp(&right.source_object_id.value)
                .then_with(|| left.source_version.cmp(&right.source_version))
        });

        let next_cursor = events
            .iter()
            .filter_map(|event| event.source_version.as_deref())
            .max()
            .map(str::to_owned)
            .map(Cursor);

        Ok(ChangeBatch {
            events,
            next_cursor,
        })
    }

    fn import(&self) -> DomainResult<ChangeBatch> {
        if self.path.is_dir() {
            return import_directory(&self.path);
        }

        let file = File::open(&self.path)
            .map_err(|error| DomainError(format!("open Claude export: {error}")))?;
        let mut archive = ZipArchive::new(file)
            .map_err(|error| DomainError(format!("read Claude export archive: {error}")))?;
        let mut all_events = Vec::new();

        for index in 0..archive.len() {
            let mut entry = archive
                .by_index(index)
                .map_err(|error| DomainError(format!("read Claude export entry: {error}")))?;
            if !entry.is_file() || !entry.name().ends_with(".json") {
                continue;
            }

            let entry_name = entry.name().to_owned();
            let mut contents = String::new();
            entry
                .read_to_string(&mut contents)
                .map_err(|error| DomainError(format!("read {entry_name}: {error}")))?;
            let value: Value = serde_json::from_str(&contents)
                .map_err(|error| DomainError(format!("parse {entry_name}: {error}")))?;
            all_events.extend(Self::parse_json(&value)?.events);
        }

        Ok(deduplicate_events(all_events))
    }
}

#[async_trait]
impl Connector for ClaudeArtifactConnector {
    fn id(&self) -> ConnectorId {
        self.id.clone()
    }

    fn source_system(&self) -> SourceSystem {
        SourceSystem::Claude
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_webhooks: false,
            supports_incremental_polling: false,
            supports_historical_backfill: true,
            supports_deletions: false,
            supports_permissions: true,
            supports_reconciliation: true,
        }
    }

    async fn validate(&self) -> DomainResult<ConnectorHealth> {
        let authenticated = self.path.exists();
        Ok(ConnectorHealth {
            connector_id: self.id.clone(),
            source_system: SourceSystem::Claude,
            authenticated,
            message: Some(if authenticated {
                "Claude export is available".into()
            } else {
                "Claude export path does not exist".into()
            }),
        })
    }

    async fn backfill(&self, _request: BackfillRequest) -> DomainResult<ChangeBatch> {
        self.import()
    }

    async fn poll(&self, _cursor: Option<Cursor>) -> DomainResult<ChangeBatch> {
        self.import()
    }

    async fn handle_webhook(&self, _request: WebhookRequest) -> DomainResult<ChangeBatch> {
        Err(unsupported("Claude artifact webhooks"))
    }

    async fn reconcile(&self, _request: ReconcileRequest) -> DomainResult<ChangeBatch> {
        self.import()
    }
}

fn import_directory(path: &Path) -> DomainResult<ChangeBatch> {
    let mut all_events = Vec::new();
    let entries = std::fs::read_dir(path)
        .map_err(|error| DomainError(format!("read Claude export directory: {error}")))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| DomainError(format!("read Claude export file: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let contents = std::fs::read_to_string(&path)
            .map_err(|error| DomainError(format!("read {}: {error}", path.display())))?;
        let value: Value = serde_json::from_str(&contents)
            .map_err(|error| DomainError(format!("parse {}: {error}", path.display())))?;
        all_events.extend(ClaudeArtifactConnector::parse_json(&value)?.events);
    }
    Ok(deduplicate_events(all_events))
}

fn collect_events(value: &Value, events: &mut Vec<SourceEvent>) -> DomainResult<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_events(value, events)?;
            }
        }
        Value::Object(object) => {
            if let Some(event) = event_from_artifact(object)? {
                events.push(event);
            }
            for value in object.values() {
                collect_events(value, events)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn event_from_artifact(
    object: &serde_json::Map<String, Value>,
) -> DomainResult<Option<SourceEvent>> {
    let Some(id) = string_field(object, &["id", "artifactId", "artifact_id"]) else {
        return Ok(None);
    };
    if !is_artifact_record(object) || !is_organization_visible(object) {
        return Ok(None);
    }

    let version = string_or_number_field(
        object,
        &[
            "version",
            "versionId",
            "version_id",
            "updatedAt",
            "updated_at",
        ],
    );
    Ok(Some(SourceEvent {
        connector_id: ConnectorId(CONNECTOR_ID.into()),
        source_object_id: SourceObjectId {
            system: SourceSystem::Claude,
            value: id,
        },
        resource_type: "claude_artifact".into(),
        operation: ObservationOperation::Upsert,
        source_version: version.clone(),
        source_timestamp: version,
        observed_at: runtime_timestamp(),
        payload: Some(Value::Object(object.clone()).to_string()),
        permissions: vec![],
    }))
}

fn is_artifact_record(object: &serde_json::Map<String, Value>) -> bool {
    object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("artifact"))
        || ["content", "artifactContent", "artifact_content", "code"]
            .iter()
            .any(|field| object.contains_key(*field))
}

fn is_organization_visible(object: &serde_json::Map<String, Value>) -> bool {
    let visibility = string_field(object, &["visibility", "sharing", "shareScope"])
        .map(|value| value.to_ascii_lowercase());
    matches!(
        visibility.as_deref(),
        Some("organization") | Some("organization-wide") | Some("org")
    ) || object
        .get("sharedWithOrganization")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn string_field(object: &serde_json::Map<String, Value>, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        object
            .get(*field)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn string_or_number_field(
    object: &serde_json::Map<String, Value>,
    fields: &[&str],
) -> Option<String> {
    fields.iter().find_map(|field| {
        object.get(*field).and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
    })
}

fn deduplicate_events(events: Vec<SourceEvent>) -> ChangeBatch {
    let mut unique = std::collections::BTreeMap::new();
    for event in events {
        unique.insert(
            (
                event.source_object_id.value.clone(),
                event.source_version.clone(),
            ),
            event,
        );
    }
    let events = unique.into_values().collect::<Vec<_>>();
    let next_cursor = events
        .iter()
        .filter_map(|event| event.source_version.as_deref())
        .max()
        .map(str::to_owned)
        .map(Cursor);
    ChangeBatch {
        events,
        next_cursor,
    }
}

fn runtime_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn imports_only_organization_visible_artifacts() {
        let batch = ClaudeArtifactConnector::parse_json(&json!({
            "artifacts": [
                {"id": "org-1", "type": "artifact", "visibility": "organization", "version": 2, "content": "shared"},
                {"id": "private-1", "type": "artifact", "visibility": "private", "content": "secret"},
                {"id": "unknown-1", "type": "artifact", "content": "unknown"}
            ]
        }))
        .expect("batch");

        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].source_object_id.value, "org-1");
        assert_eq!(
            batch.events[0].source_object_id.system,
            SourceSystem::Claude
        );
    }
}
