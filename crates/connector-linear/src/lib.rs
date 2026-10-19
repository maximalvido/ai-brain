//! Read-only Linear GraphQL connector.

use async_trait::async_trait;
use brain_connectors::{
    BackfillRequest, ChangeBatch, Connector, ConnectorCapabilities, ConnectorHealth, Cursor,
    ReconcileRequest, WebhookRequest, unsupported,
};
use brain_domain::{
    ConnectorId, DomainError, DomainResult, ObservationOperation, SourceEvent, SourceObjectId,
    SourceSystem,
};
use reqwest::Client;
use serde_json::{Value, json};

const LINEAR_ENDPOINT: &str = "https://api.linear.app/graphql";
const PAGE_SIZE: u64 = 100;

pub struct LinearConnector {
    id: ConnectorId,
    api_key: String,
    client: Client,
}

impl LinearConnector {
    pub fn new(id: impl Into<String>, api_key: impl Into<String>) -> DomainResult<Self> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(DomainError("Linear API key is empty".into()));
        }

        Ok(Self {
            id: ConnectorId(id.into()),
            api_key,
            client: Client::new(),
        })
    }

    pub fn from_env() -> DomainResult<Self> {
        dotenvy::dotenv().ok();
        let api_key = std::env::var("LINEAR_API_KEY")
            .map_err(|_| DomainError("LINEAR_API_KEY is not configured".into()))?;
        Self::new("linear-main", api_key)
    }

    async fn query(&self, query: &str, variables: Value) -> DomainResult<Value> {
        let response = self
            .client
            .post(LINEAR_ENDPOINT)
            .header("Authorization", &self.api_key)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .map_err(|error| DomainError(format!("Linear request failed: {error}")))?;

        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|error| DomainError(format!("Linear response was not JSON: {error}")))?;

        if !status.is_success() {
            return Err(DomainError(format!(
                "Linear returned HTTP {status}: {body}"
            )));
        }

        if let Some(errors) = body.get("errors") {
            return Err(DomainError(format!("Linear GraphQL errors: {errors}")));
        }

        body.get("data")
            .cloned()
            .ok_or_else(|| DomainError("Linear response did not include data".into()))
    }

    async fn fetch_collection(
        &self,
        field: &str,
        selection: &str,
        resource_type: &str,
    ) -> DomainResult<Vec<SourceEvent>> {
        let query = format!(
            "query($first: Int!, $after: String) {{ {field}(first: $first, after: $after) {{ nodes {{ {selection} }} pageInfo {{ hasNextPage endCursor }} }} }}"
        );
        let mut after = None;
        let mut events = Vec::new();

        loop {
            let variables = json!({ "first": PAGE_SIZE, "after": after });
            let data = self.query(&query, variables).await?;
            let connection = data
                .get(field)
                .ok_or_else(|| DomainError(format!("Linear response missing field: {field}")))?;
            let nodes = connection
                .get("nodes")
                .and_then(Value::as_array)
                .ok_or_else(|| DomainError(format!("Linear response missing nodes: {field}")))?;

            events.extend(
                nodes
                    .iter()
                    .map(|node| event_from_node(&self.id, resource_type, node))
                    .collect::<DomainResult<Vec<_>>>()?,
            );

            let has_next = connection
                .get("pageInfo")
                .and_then(|info| info.get("hasNextPage"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !has_next {
                break;
            }
            after = connection
                .get("pageInfo")
                .and_then(|info| info.get("endCursor"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if after.is_none() {
                return Err(DomainError(format!(
                    "Linear page {field} reported another page without a cursor"
                )));
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Connector for LinearConnector {
    fn id(&self) -> ConnectorId {
        self.id.clone()
    }

    fn source_system(&self) -> SourceSystem {
        SourceSystem::Linear
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
        let data = self
            .query(
                "query { viewer { id name email } }",
                Value::Object(Default::default()),
            )
            .await?;
        let authenticated = data.get("viewer").is_some_and(|viewer| !viewer.is_null());
        Ok(ConnectorHealth {
            connector_id: self.id.clone(),
            source_system: SourceSystem::Linear,
            authenticated,
            message: authenticated.then(|| "Linear authentication succeeded".into()),
        })
    }

    async fn backfill(&self, request: BackfillRequest) -> DomainResult<ChangeBatch> {
        let requested = if request.resource_types.is_empty() {
            vec!["Team", "Project", "Issue"]
        } else {
            request.resource_types.iter().map(String::as_str).collect()
        };
        let mut events = Vec::new();

        if requested.contains(&"Team") {
            events.extend(
                self.fetch_collection("teams", "id name key description updatedAt", "team")
                    .await?,
            );
        }
        if requested.contains(&"Project") {
            events.extend(
                self.fetch_collection(
                    "projects",
                    "id name description url status { name } updatedAt",
                    "project",
                )
                .await?,
            );
        }
        if requested.contains(&"Issue") {
            events.extend(
                self.fetch_collection(
                    "issues",
                    "id identifier title description url priority priorityLabel state { id name type } team { id name key } assignee { id name email } project { id name } updatedAt archivedAt",
                    "issue",
                )
                .await?,
            );
        }

        Ok(ChangeBatch {
            events,
            next_cursor: None,
        })
    }

    async fn poll(&self, _cursor: Option<Cursor>) -> DomainResult<ChangeBatch> {
        self.backfill(BackfillRequest {
            resource_types: vec!["Team".into(), "Project".into(), "Issue".into()],
            since: None,
        })
        .await
    }

    async fn handle_webhook(&self, _request: WebhookRequest) -> DomainResult<ChangeBatch> {
        Err(unsupported("Linear webhooks"))
    }

    async fn reconcile(&self, _request: ReconcileRequest) -> DomainResult<ChangeBatch> {
        self.backfill(BackfillRequest::default()).await
    }
}

fn event_from_node(
    connector_id: &ConnectorId,
    resource_type: &str,
    node: &Value,
) -> DomainResult<SourceEvent> {
    let id = node
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| DomainError(format!("Linear {resource_type} has no id")))?;
    let updated_at = node
        .get("updatedAt")
        .and_then(Value::as_str)
        .map(str::to_owned);

    Ok(SourceEvent {
        connector_id: connector_id.clone(),
        source_object_id: SourceObjectId {
            system: SourceSystem::Linear,
            value: id.into(),
        },
        resource_type: resource_type.into(),
        operation: ObservationOperation::Upsert,
        source_version: updated_at,
        source_timestamp: None,
        observed_at: runtime_timestamp(),
        payload: Some(node.to_string()),
        permissions: vec![],
    })
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

    #[test]
    fn converts_linear_node_to_source_event() {
        let connector_id = ConnectorId("linear-main".into());
        let event = event_from_node(
            &connector_id,
            "issue",
            &json!({ "id": "issue-1", "updatedAt": "2026-10-17T00:00:00Z" }),
        )
        .expect("event");

        assert_eq!(event.source_object_id.value, "issue-1");
        assert_eq!(event.resource_type, "issue");
        assert_eq!(
            event.source_version.as_deref(),
            Some("2026-10-17T00:00:00Z")
        );
        assert_eq!(event.source_object_id.system, SourceSystem::Linear);
    }
}
