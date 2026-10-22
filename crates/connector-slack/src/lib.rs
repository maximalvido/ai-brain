//! Importer for Slack JSON workspace exports.
//!
//! The importer supports the documented JSON export layout: conversation
//! metadata files plus date-partitioned message files under public channels.
//! The live API connector is scope-controlled and does not download linked
//! Slack files.

use async_trait::async_trait;
use brain_connectors::{
    BackfillRequest, ChangeBatch, Connector, ConnectorCapabilities, ConnectorHealth, Cursor,
    ReconcileRequest, WebhookRequest, unsupported,
};
use brain_domain::{
    ConnectorId, DomainError, DomainResult, ObservationOperation, SourceEvent, SourceObjectId,
    SourceSystem,
};
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use zip::ZipArchive;

const CONNECTOR_ID: &str = "slack-export";
const API_CONNECTOR_ID: &str = "slack-api";
const SLACK_API: &str = "https://slack.com/api";
const RATE_LIMIT_ATTEMPTS: u32 = 5;
const DEFAULT_RETRY_AFTER_SECONDS: u64 = 60;
const MAX_RETRY_AFTER_SECONDS: u64 = 300;

#[derive(Clone, Debug, Default)]
struct Conversation {
    id: String,
    name: String,
    kind: String,
}

pub struct SlackExportConnector {
    path: PathBuf,
    id: ConnectorId,
}

pub struct SlackApiConnector {
    token: String,
    conversation_types: String,
    team_id: Option<String>,
    initial_oldest: String,
    client: reqwest::Client,
    id: ConnectorId,
}

impl SlackApiConnector {
    pub fn from_env() -> DomainResult<Self> {
        dotenvy::dotenv().ok();
        let token = std::env::var("SLACK_API_TOKEN")
            .map_err(|_| DomainError("SLACK_API_TOKEN is not configured".into()))?;
        if token.trim().is_empty() {
            return Err(DomainError("SLACK_API_TOKEN is empty".into()));
        }
        let lookback = std::env::var("SLACK_INITIAL_LOOKBACK_SECONDS")
            .unwrap_or_else(|_| "604800".into())
            .parse::<u64>()
            .map_err(|_| DomainError("SLACK_INITIAL_LOOKBACK_SECONDS must be an integer".into()))?;
        let oldest = unix_timestamp().saturating_sub(lookback);
        Ok(Self {
            token,
            conversation_types: std::env::var("SLACK_CONVERSATION_TYPES")
                .unwrap_or_else(|_| "public_channel".into()),
            team_id: std::env::var("SLACK_TEAM_ID")
                .ok()
                .filter(|value| !value.is_empty()),
            initial_oldest: format!("{oldest}.000000"),
            client: reqwest::Client::new(),
            id: ConnectorId(API_CONNECTOR_ID.into()),
        })
    }

    async fn call(&self, method: &str, parameters: Value) -> DomainResult<Value> {
        let mut attempt = 1;
        let response = loop {
            let response = self
                .client
                .post(format!("{SLACK_API}/{method}"))
                .bearer_auth(&self.token)
                .json(&parameters)
                .send()
                .await
                .map_err(|error| DomainError(format!("Slack request failed: {error}")))?;
            if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS
                || attempt >= RATE_LIMIT_ATTEMPTS
            {
                break response;
            }
            let delay = retry_after_seconds(
                response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
            );
            eprintln!("Slack rate limited; method={method} attempt={attempt} retry_after={delay}s");
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            attempt += 1;
        };
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|error| DomainError(format!("Slack response was not JSON: {error}")))?;
        if !status.is_success() {
            return Err(DomainError(format!("Slack returned HTTP {status}: {body}")));
        }
        if !body.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            return Err(DomainError(format!("Slack API error: {body}")));
        }
        Ok(body)
    }

    async fn list_conversations(&self) -> DomainResult<Vec<Conversation>> {
        let mut cursor: Option<String> = None;
        let mut conversations = Vec::new();
        loop {
            let mut parameters = serde_json::json!({
                "exclude_archived": false,
                "limit": 200,
                "types": self.conversation_types,
            });
            if let Some(team_id) = &self.team_id {
                parameters["team_id"] = Value::String(team_id.clone());
            }
            if let Some(cursor) = &cursor {
                parameters["cursor"] = Value::String(cursor.clone());
            }
            let body = self.call("conversations.list", parameters).await?;
            let items = body
                .get("channels")
                .and_then(Value::as_array)
                .ok_or_else(|| DomainError("Slack response omitted channels".into()))?;
            conversations.extend(
                items
                    .iter()
                    .filter(|item| is_readable_conversation(item))
                    .filter_map(conversation_from_api),
            );
            cursor = next_cursor(&body);
            if cursor.is_none() {
                break;
            }
        }
        Ok(conversations)
    }

    async fn history(
        &self,
        conversation: &Conversation,
        oldest: &str,
    ) -> DomainResult<Vec<SourceEvent>> {
        let mut cursor: Option<String> = None;
        let mut events = Vec::new();
        loop {
            let mut parameters = serde_json::json!({
                "channel": conversation.id,
                "inclusive": true,
                "limit": 15,
                "oldest": oldest,
            });
            if let Some(cursor) = &cursor {
                parameters["cursor"] = Value::String(cursor.clone());
            }
            let body = self.call("conversations.history", parameters).await?;
            let messages = body
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| DomainError("Slack response omitted messages".into()))?;
            for message in messages {
                if let Some(event) = event_from_api_message(message, conversation)? {
                    events.push(event);
                }
            }
            cursor = next_cursor(&body);
            if cursor.is_none() {
                break;
            }
        }
        Ok(events)
    }
}

#[async_trait]
impl Connector for SlackApiConnector {
    fn id(&self) -> ConnectorId {
        self.id.clone()
    }

    fn source_system(&self) -> SourceSystem {
        SourceSystem::Slack
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_webhooks: true,
            supports_incremental_polling: true,
            supports_historical_backfill: true,
            supports_deletions: true,
            supports_permissions: false,
            supports_reconciliation: true,
        }
    }

    async fn validate(&self) -> DomainResult<ConnectorHealth> {
        let body = self.call("auth.test", serde_json::json!({})).await?;
        Ok(ConnectorHealth {
            connector_id: self.id.clone(),
            source_system: SourceSystem::Slack,
            authenticated: body.get("ok").and_then(Value::as_bool).unwrap_or(false),
            message: body
                .get("team")
                .and_then(Value::as_str)
                .map(|team| format!("Slack authentication succeeded for {team}")),
        })
    }

    async fn backfill(&self, _request: BackfillRequest) -> DomainResult<ChangeBatch> {
        self.poll(None).await
    }

    async fn poll(&self, cursor: Option<Cursor>) -> DomainResult<ChangeBatch> {
        let oldest = cursor
            .map(|cursor| cursor.0)
            .unwrap_or_else(|| self.initial_oldest.clone());
        let conversations = self.list_conversations().await?;
        let mut events = Vec::new();
        let mut failures = 0;
        for conversation in conversations {
            match self.history(&conversation, &oldest).await {
                Ok(history) => events.extend(history),
                Err(error) => {
                    failures += 1;
                    eprintln!(
                        "Slack history sync failed; conversation_id={} conversation_name={} error={error}",
                        conversation.id, conversation.name
                    );
                }
            }
        }
        let mut batch = deduplicate_events(events);
        if failures > 0 {
            // Keep the window open so failed conversations are retried on the next run.
            eprintln!(
                "Slack sync completed with partial failures; failed_conversations={failures} cursor_held={oldest}"
            );
            batch.next_cursor = Some(Cursor(oldest));
        }
        Ok(batch)
    }

    async fn handle_webhook(&self, request: WebhookRequest) -> DomainResult<ChangeBatch> {
        let secret = std::env::var("SLACK_SIGNING_SECRET")
            .map_err(|_| DomainError("SLACK_SIGNING_SECRET is not configured".into()))?;
        verify_slack_signature(
            &secret,
            &request.body,
            request.signature.as_deref(),
            request
                .headers
                .get("x-slack-request-timestamp")
                .map(String::as_str),
        )?;

        let payload: Value = serde_json::from_str(&request.body)
            .map_err(|error| DomainError(format!("Slack webhook is not JSON: {error}")))?;
        if payload.get("type").and_then(Value::as_str) == Some("url_verification") {
            return Ok(ChangeBatch::default());
        }
        if payload.get("type").and_then(Value::as_str) != Some("event_callback") {
            return Ok(ChangeBatch::default());
        }
        let event = payload
            .get("event")
            .ok_or_else(|| DomainError("Slack event callback omitted event".into()))?;
        slack_webhook_event(event).map(|event| ChangeBatch {
            events: event.into_iter().collect(),
            next_cursor: None,
        })
    }

    async fn reconcile(&self, _request: ReconcileRequest) -> DomainResult<ChangeBatch> {
        self.poll(None).await
    }
}

impl SlackExportConnector {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            id: ConnectorId(CONNECTOR_ID.into()),
        }
    }

    pub fn parse_export(entries: &[(String, Value)]) -> DomainResult<ChangeBatch> {
        let conversations = conversation_index(entries);
        let mut events = Vec::new();
        for (path, value) in entries {
            if !is_message_file(path) {
                continue;
            }
            let conversation = conversation_from_path(path, &conversations);
            let messages = match value {
                Value::Array(messages) => messages,
                _ => continue,
            };
            for message in messages {
                if let Some(event) = event_from_message(message, conversation.as_ref())? {
                    events.push(event);
                }
            }
        }

        Ok(deduplicate_events(events))
    }

    fn import(&self) -> DomainResult<ChangeBatch> {
        if self.path.is_dir() {
            return import_directory(&self.path);
        }

        let file = File::open(&self.path)
            .map_err(|error| DomainError(format!("open Slack export: {error}")))?;
        let mut archive = ZipArchive::new(file)
            .map_err(|error| DomainError(format!("read Slack export archive: {error}")))?;
        let mut entries = Vec::new();

        for index in 0..archive.len() {
            let mut entry = archive
                .by_index(index)
                .map_err(|error| DomainError(format!("read Slack export entry: {error}")))?;
            if !entry.is_file() || !entry.name().ends_with(".json") {
                continue;
            }
            let name = entry.name().to_owned();
            let mut contents = String::new();
            entry
                .read_to_string(&mut contents)
                .map_err(|error| DomainError(format!("read {name}: {error}")))?;
            let value: Value = serde_json::from_str(&contents)
                .map_err(|error| DomainError(format!("parse {name}: {error}")))?;
            entries.push((name, value));
        }

        Self::parse_export(&entries)
    }
}

#[async_trait]
impl Connector for SlackExportConnector {
    fn id(&self) -> ConnectorId {
        self.id.clone()
    }

    fn source_system(&self) -> SourceSystem {
        SourceSystem::Slack
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_webhooks: false,
            supports_incremental_polling: false,
            supports_historical_backfill: true,
            supports_deletions: false,
            supports_permissions: false,
            supports_reconciliation: true,
        }
    }

    async fn validate(&self) -> DomainResult<ConnectorHealth> {
        let available = self.path.exists();
        Ok(ConnectorHealth {
            connector_id: self.id.clone(),
            source_system: SourceSystem::Slack,
            authenticated: available,
            message: Some(if available {
                "Slack export is available".into()
            } else {
                "Slack export path does not exist".into()
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
        Err(unsupported("Slack webhooks"))
    }

    async fn reconcile(&self, _request: ReconcileRequest) -> DomainResult<ChangeBatch> {
        self.import()
    }
}

fn import_directory(path: &Path) -> DomainResult<ChangeBatch> {
    let mut entries = Vec::new();
    collect_json_files(path, path, &mut entries)?;
    SlackExportConnector::parse_export(&entries)
}

fn collect_json_files(
    root: &Path,
    path: &Path,
    entries: &mut Vec<(String, Value)>,
) -> DomainResult<()> {
    for entry in std::fs::read_dir(path)
        .map_err(|error| DomainError(format!("read Slack export directory: {error}")))?
    {
        let entry =
            entry.map_err(|error| DomainError(format!("read Slack export file: {error}")))?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(root, &path, entries)?;
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let contents = std::fs::read_to_string(&path)
            .map_err(|error| DomainError(format!("read {}: {error}", path.display())))?;
        let value: Value = serde_json::from_str(&contents)
            .map_err(|error| DomainError(format!("parse {}: {error}", path.display())))?;
        let name = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        entries.push((name, value));
    }
    Ok(())
}

fn conversation_index(entries: &[(String, Value)]) -> BTreeMap<String, Conversation> {
    let mut index = BTreeMap::new();
    for (path, value) in entries {
        let Some(kind) = conversation_metadata_kind(path) else {
            continue;
        };
        let Some(items) = value.as_array() else {
            continue;
        };
        for item in items {
            let Some(object) = item.as_object() else {
                continue;
            };
            let Some(id) = string_field(object, &["id"]) else {
                continue;
            };
            let name = string_field(object, &["name", "user"]).unwrap_or_else(|| id.clone());
            index.insert(
                id.clone(),
                Conversation {
                    id,
                    name,
                    kind: kind.into(),
                },
            );
        }
    }
    index
}

fn conversation_metadata_kind(path: &str) -> Option<&'static str> {
    match Path::new(path).file_name().and_then(|name| name.to_str()) {
        Some("channels.json") => Some("channel"),
        Some("groups.json") => Some("private_channel"),
        Some("dms.json") => Some("dm"),
        Some("mpims.json") => Some("group_dm"),
        _ => None,
    }
}

fn conversation_from_api(value: &Value) -> Option<Conversation> {
    let object = value.as_object()?;
    let id = string_field(object, &["id"])?;
    Some(Conversation {
        name: string_field(object, &["name", "user"]).unwrap_or_else(|| id.clone()),
        id,
        kind: if object
            .get("is_im")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "dm"
        } else if object
            .get("is_mpim")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "group_dm"
        } else if object
            .get("is_private")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "private_channel"
        } else {
            "channel"
        }
        .into(),
    })
}

fn next_cursor(body: &Value) -> Option<String> {
    body.get("response_metadata")
        .and_then(|metadata| metadata.get("next_cursor"))
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
        .map(str::to_owned)
}

fn verify_slack_signature(
    secret: &str,
    body: &str,
    signature: Option<&str>,
    timestamp: Option<&str>,
) -> DomainResult<()> {
    let signature =
        signature.ok_or_else(|| DomainError("Slack webhook signature is missing".into()))?;
    let timestamp =
        timestamp.ok_or_else(|| DomainError("Slack webhook timestamp is missing".into()))?;
    let timestamp_seconds = timestamp
        .parse::<u64>()
        .map_err(|_| DomainError("Slack webhook timestamp is invalid".into()))?;
    if unix_timestamp().abs_diff(timestamp_seconds) > 300 {
        return Err(DomainError("Slack webhook timestamp is too old".into()));
    }
    let base = format!("v0:{timestamp}:{body}");
    let expected = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| DomainError("Slack signing secret is invalid".into()))?
        .chain_update(base.as_bytes())
        .finalize()
        .into_bytes();
    let expected = format!("v0={}", hex_encode(&expected));
    if constant_time_equal(expected.as_bytes(), signature.as_bytes()) {
        Ok(())
    } else {
        Err(DomainError("Slack webhook signature is invalid".into()))
    }
}

fn slack_webhook_event(value: &Value) -> DomainResult<Option<SourceEvent>> {
    let object = value
        .as_object()
        .ok_or_else(|| DomainError("Slack webhook event is not an object".into()))?;
    let event_type = object.get("type").and_then(Value::as_str).unwrap_or("");
    let subtype = object.get("subtype").and_then(Value::as_str);
    let channel_type = object
        .get("channel_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let channel = string_field(object, &["channel"]);
    if channel_type != "channel" && !channel.as_deref().is_some_and(|id| id.starts_with('C')) {
        return Ok(None);
    }
    let (message, operation) = match (event_type, subtype) {
        ("message", Some("message_changed")) => (
            object.get("message").unwrap_or(value),
            ObservationOperation::Upsert,
        ),
        ("message", Some("message_deleted")) => (value, ObservationOperation::Delete),
        ("message", _) => (value, ObservationOperation::Upsert),
        _ => return Ok(None),
    };
    let message_object = message
        .as_object()
        .ok_or_else(|| DomainError("Slack webhook message is not an object".into()))?;
    let timestamp = string_field(message_object, &["ts", "deleted_ts"])
        .or_else(|| string_field(object, &["event_ts"]))
        .ok_or_else(|| DomainError("Slack webhook message omitted timestamp".into()))?;
    let channel = channel.ok_or_else(|| DomainError("Slack webhook omitted channel".into()))?;
    let mut payload = object.clone();
    payload.insert(
        "_brain_conversation".into(),
        serde_json::json!({
            "id": channel,
            "kind": "channel",
        }),
    );
    Ok(Some(SourceEvent {
        connector_id: ConnectorId(API_CONNECTOR_ID.into()),
        source_object_id: SourceObjectId {
            system: SourceSystem::Slack,
            value: format!("{}:{timestamp}", channel),
        },
        resource_type: "slack_message".into(),
        operation,
        source_version: Some(timestamp.clone()),
        source_timestamp: Some(timestamp),
        observed_at: runtime_timestamp(),
        payload: Some(Value::Object(payload).to_string()),
        permissions: vec![],
    }))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

fn event_from_api_message(
    value: &Value,
    conversation: &Conversation,
) -> DomainResult<Option<SourceEvent>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let Some(timestamp) = string_field(object, &["ts"]) else {
        return Ok(None);
    };
    let mut payload = object.clone();
    payload.insert(
        "_brain_conversation".into(),
        serde_json::json!({
            "id": conversation.id,
            "name": conversation.name,
            "kind": conversation.kind,
        }),
    );
    Ok(Some(SourceEvent {
        connector_id: ConnectorId(API_CONNECTOR_ID.into()),
        source_object_id: SourceObjectId {
            system: SourceSystem::Slack,
            value: format!("{}:{timestamp}", conversation.id),
        },
        resource_type: "slack_message".into(),
        operation: ObservationOperation::Upsert,
        source_version: Some(timestamp.clone()),
        source_timestamp: Some(timestamp),
        observed_at: runtime_timestamp(),
        payload: Some(Value::Object(payload).to_string()),
        permissions: vec![],
    }))
}

fn conversation_from_path(
    path: &str,
    conversations: &BTreeMap<String, Conversation>,
) -> Option<Conversation> {
    let components = Path::new(path)
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>();
    let collection_index = components
        .iter()
        .position(|component| *component == "channels")?;
    let name = components.get(collection_index + 1)?.to_string();
    conversations
        .values()
        .find(|conversation| conversation.name == name)
        .cloned()
        .or_else(|| {
            Some(Conversation {
                id: name.clone(),
                name,
                kind: "channel".into(),
            })
        })
}

fn is_message_file(path: &str) -> bool {
    path.ends_with(".json")
        && conversation_metadata_kind(path).is_none()
        && Path::new(path)
            .components()
            .any(|component| matches!(component.as_os_str().to_str(), Some("channels")))
}

fn event_from_message(
    value: &Value,
    conversation: Option<&Conversation>,
) -> DomainResult<Option<SourceEvent>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let Some(timestamp) = string_field(object, &["ts", "timestamp"]) else {
        return Ok(None);
    };
    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind != "message")
    {
        return Ok(None);
    }
    let conversation = conversation.cloned().unwrap_or_default();
    let conversation_id = if conversation.id.is_empty() {
        "unknown"
    } else {
        conversation.id.as_str()
    };
    let mut payload = object.clone();
    payload.insert(
        "_brain_conversation".into(),
        serde_json::json!({
            "id": conversation_id,
            "name": conversation.name,
            "kind": conversation.kind,
        }),
    );
    Ok(Some(SourceEvent {
        connector_id: ConnectorId(CONNECTOR_ID.into()),
        source_object_id: SourceObjectId {
            system: SourceSystem::Slack,
            value: format!("{conversation_id}:{timestamp}"),
        },
        resource_type: "slack_message".into(),
        operation: ObservationOperation::Upsert,
        source_version: Some(timestamp.clone()),
        source_timestamp: Some(timestamp),
        observed_at: runtime_timestamp(),
        payload: Some(Value::Object(payload).to_string()),
        permissions: vec![],
    }))
}

fn string_field(object: &Map<String, Value>, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        object
            .get(*field)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn deduplicate_events(events: Vec<SourceEvent>) -> ChangeBatch {
    let mut unique = BTreeMap::new();
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

/// Bot tokens can only read conversations they belong to; `is_member` is absent
/// for token types where membership does not apply.
fn is_readable_conversation(value: &Value) -> bool {
    value
        .get("is_member")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn retry_after_seconds(header: Option<&str>) -> u64 {
    header
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RETRY_AFTER_SECONDS)
        .clamp(1, MAX_RETRY_AFTER_SECONDS)
}

fn unix_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn imports_messages_from_export_layout() {
        let entries = vec![
            (
                "channels.json".into(),
                json!([{"id":"C1","name":"founders"}]),
            ),
            ("groups.json".into(), json!([{"id":"G1","name":"private"}])),
            ("dms.json".into(), json!([{"id":"D1","user":"U2"}])),
            (
                "channels/founders/2026-10-20.json".into(),
                json!([
                    {"type":"message","user":"U1","text":"hello","ts":"1.000001"},
                    {"type":"message","user":"U1","text":"thread","ts":"2.000001"},
                    {"type":"channel_join","ts":"3.000001"}
                ]),
            ),
            (
                "groups/private/2026-10-20.json".into(),
                json!([{"type":"message","user":"U2","text":"private","ts":"4.000001"}]),
            ),
            (
                "dms/U2/2026-10-20.json".into(),
                json!([{"type":"message","user":"U2","text":"dm","ts":"5.000001"}]),
            ),
        ];

        let batch = SlackExportConnector::parse_export(&entries).expect("batch");
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].source_object_id.value, "C1:1.000001");
        assert_eq!(batch.events[0].source_object_id.system, SourceSystem::Slack);
        assert_eq!(batch.events[0].resource_type, "slack_message");
    }

    #[test]
    fn maps_public_channel_edits_and_deletions() {
        let edited = slack_webhook_event(&json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "channel_type": "channel",
            "message": {"ts": "10.000001", "text": "edited"}
        }))
        .expect("edited event")
        .expect("edited message");
        assert_eq!(edited.operation, ObservationOperation::Upsert);
        assert_eq!(edited.source_object_id.value, "C1:10.000001");

        let deleted = slack_webhook_event(&json!({
            "type": "message",
            "subtype": "message_deleted",
            "channel": "C1",
            "channel_type": "channel",
            "deleted_ts": "10.000001"
        }))
        .expect("deleted event")
        .expect("deleted message");
        assert_eq!(deleted.operation, ObservationOperation::Delete);
        assert_eq!(deleted.source_object_id.value, "C1:10.000001");
    }

    #[test]
    fn skips_conversations_without_membership() {
        assert!(is_readable_conversation(
            &json!({"id": "C1", "is_member": true})
        ));
        assert!(!is_readable_conversation(
            &json!({"id": "C2", "is_member": false})
        ));
        assert!(is_readable_conversation(&json!({"id": "C3"})));
    }

    #[test]
    fn bounds_retry_after_delay() {
        assert_eq!(retry_after_seconds(Some("30")), 30);
        assert_eq!(retry_after_seconds(Some("0")), 1);
        assert_eq!(retry_after_seconds(Some("9999")), MAX_RETRY_AFTER_SECONDS);
        assert_eq!(
            retry_after_seconds(Some("soon")),
            DEFAULT_RETRY_AFTER_SECONDS
        );
        assert_eq!(retry_after_seconds(None), DEFAULT_RETRY_AFTER_SECONDS);
    }
}
