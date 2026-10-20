use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use brain_compiler::compile_and_write;
use brain_connectors::{Connector, Cursor, WebhookRequest};
use brain_domain::PrincipalId;
use brain_ingestion::{IngestionService, observation_for};
use brain_storage::{ResourceSearchQuery, ResourceSearchRepository};
use brain_storage_postgres::{PgObservationRepository, PgResourceWriter, connect_and_migrate};
use connector_claude::ClaudeArtifactConnector;
use connector_linear::LinearConnector;
use connector_slack::SlackApiConnector;
use connector_slack::SlackExportConnector;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::io::BufRead;
use std::net::SocketAddr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        match std::env::args().nth(1).as_deref() {
            Some("sync") => sync_linear().await,
            Some("sync-loop") => sync_linear_loop().await,
            Some("import-claude-artifacts") => import_claude_artifacts().await,
            Some("import-slack-export") => import_slack_export().await,
            Some("sync-slack") => sync_slack().await,
            Some("sync-slack-loop") => sync_slack_loop().await,
            Some("validate-linear") => validate_linear().await,
            Some("search") => search_resources().await,
            Some("mcp") => mcp_server().await,
            Some("serve-http") => serve_http().await,
            _ => {
                println!("Usage: brain-server <sync|sync-loop|sync-slack|sync-slack-loop|import-claude-artifacts <export.zip>|import-slack-export <export.zip>|validate-linear|search <text>|mcp|serve-http>");
                Ok(())
            }
        }
    })
}

#[derive(Clone)]
struct HttpState {
    pool: PgPool,
    repository: PgResourceWriter,
    access_key: String,
}

async fn serve_http() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    let state = HttpState {
        pool: pool.clone(),
        repository: PgResourceWriter::new(pool.clone()),
        access_key: configured_access_key()?,
    };
    let address = std::env::var("BRAIN_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8787".into())
        .parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("AI Brain HTTP MCP listening on http://{address}/mcp");
    axum::serve(
        listener,
        Router::new()
            .route("/mcp", post(http_mcp))
            .route("/webhooks/linear", post(linear_webhook))
            .route("/webhooks/slack", post(slack_webhook))
            .with_state(state),
    )
    .await?;
    Ok(())
}

async fn linear_webhook(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_text = String::from_utf8_lossy(&body).into_owned();
    let request = webhook_request(&headers, body_text.clone(), "linear-signature");
    let connector = match LinearConnector::from_env() {
        Ok(connector) => connector,
        Err(error) => return webhook_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    match process_webhook(&state, &connector, request).await {
        Ok((inserted, duplicates)) => (
            StatusCode::OK,
            Json(json!({"ok": true, "inserted": inserted, "duplicates": duplicates})),
        )
            .into_response(),
        Err(error) => webhook_error(webhook_error_status(&error), error),
    }
}

async fn slack_webhook(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_text = String::from_utf8_lossy(&body).into_owned();
    let request = webhook_request(&headers, body_text.clone(), "x-slack-signature");
    let connector = match SlackApiConnector::from_env() {
        Ok(connector) => connector,
        Err(error) => return webhook_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };
    match process_webhook(&state, &connector, request).await {
        Ok((inserted, duplicates)) => {
            let payload: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
            if payload.get("type").and_then(Value::as_str) == Some("url_verification")
                && let Some(challenge) = payload.get("challenge").and_then(Value::as_str)
            {
                return (StatusCode::OK, Json(json!({"challenge": challenge}))).into_response();
            }
            (
                StatusCode::OK,
                Json(json!({"ok": true, "inserted": inserted, "duplicates": duplicates})),
            )
                .into_response()
        }
        Err(error) => webhook_error(webhook_error_status(&error), error),
    }
}

fn webhook_request(headers: &HeaderMap, body: String, signature_header: &str) -> WebhookRequest {
    let values = headers
        .iter()
        .filter_map(|(name, value)| {
            Some((
                name.as_str().to_ascii_lowercase(),
                value.to_str().ok()?.to_owned(),
            ))
        })
        .collect();
    WebhookRequest {
        signature: headers
            .get(signature_header)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        body,
        headers: values,
    }
}

async fn process_webhook(
    state: &HttpState,
    connector: &dyn Connector,
    request: WebhookRequest,
) -> Result<(usize, usize), String> {
    let batch = connector
        .handle_webhook(request)
        .await
        .map_err(|error| error.to_string())?;
    let repository = PgObservationRepository::new(state.pool.clone());
    let ingestion = IngestionService::new(repository);
    let mut inserted = 0;
    let mut duplicates = 0;
    for event in batch.events {
        let observation = observation_for(event);
        match ingestion
            .ingest(observation.event.clone())
            .await
            .map_err(|error| error.to_string())?
        {
            brain_storage::AppendResult::Inserted(_) => inserted += 1,
            brain_storage::AppendResult::AlreadyExists(_) => duplicates += 1,
        }
        compile_and_write(&state.repository, &observation)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok((inserted, duplicates))
}

fn webhook_error_status(error: &str) -> StatusCode {
    if error.contains("signature") || error.contains("timestamp") {
        StatusCode::UNAUTHORIZED
    } else if error.contains("not JSON") || error.contains("omitted") {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn webhook_error(status: StatusCode, message: String) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

async fn http_mcp(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let principal = match authenticate_http(&state, &headers) {
        Ok(principal) => principal,
        Err(message) => {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": message}))).into_response();
        }
    };
    let response = handle_mcp_request(&state.repository, &principal, &request).await;
    match response {
        Some(response) => Json(response).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

fn authenticate_http(state: &HttpState, headers: &HeaderMap) -> Result<PrincipalId, String> {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .ok_or("Authorization bearer token is required")?;
    let token = authorization
        .strip_prefix("Bearer ")
        .or_else(|| authorization.strip_prefix("bearer "))
        .ok_or("Authorization must use the Bearer scheme")?;

    if !constant_time_equal(token.as_bytes(), state.access_key.as_bytes()) {
        return Err("invalid BRAIN_API_KEY".into());
    }
    Ok(PrincipalId("installation".into()))
}

async fn mcp_server() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    let repository = PgResourceWriter::new(pool.clone());
    let principal = PrincipalId("installation".into());
    configured_access_key()?;

    for line in std::io::stdin().lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                write_json(&json_rpc_error(Value::Null, -32700, &error.to_string()))?;
                continue;
            }
        };

        let response = handle_mcp_request(&repository, &principal, &request).await;
        if let Some(response) = response {
            write_json(&response)?;
        }
    }

    pool.close().await;
    Ok(())
}

fn configured_access_key() -> Result<String, Box<dyn std::error::Error>> {
    let key = std::env::var("BRAIN_API_KEY").map_err(|_| "BRAIN_API_KEY must be configured")?;
    if key.trim().is_empty() {
        return Err("BRAIN_API_KEY cannot be empty".into());
    }
    Ok(key)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

async fn handle_mcp_request(
    repository: &PgResourceWriter,
    principal: &PrincipalId,
    request: &Value,
) -> Option<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");

    match method {
        "notifications/initialized" => None,
        "initialize" => Some(json_rpc_result(
            id,
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "ai-brain", "version": env!("CARGO_PKG_VERSION") }
            }),
        )),
        "tools/list" => Some(json_rpc_result(
            id,
            json!({
                "tools": [{
                    "name": "brain_search",
                    "description": "Search the company's indexed knowledge and return permission-filtered source-backed resources.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string", "description": "The company question or search terms." },
                            "resource_type": { "type": "string", "description": "Optional type such as issue, project, or team." },
                            "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
                        },
                        "required": ["query"]
                    }
                }]
            }),
        )),
        "tools/call" => Some(call_brain_search(repository, principal, id, request).await),
        _ => Some(json_rpc_error(id, -32601, "Method not found")),
    }
}

async fn call_brain_search(
    repository: &PgResourceWriter,
    principal: &PrincipalId,
    id: Value,
    request: &Value,
) -> Value {
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    if name != "brain_search" {
        return json_rpc_error(id, -32602, "Unknown tool");
    }

    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let Some(query) = arguments.get("query").and_then(Value::as_str) else {
        return tool_error(id, "The query argument is required");
    };
    if query.trim().is_empty() {
        return tool_error(id, "The query argument cannot be empty");
    }

    let limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(20)
        .clamp(1, 100) as u32;
    let result = repository
        .search(
            ResourceSearchQuery {
                text: query.into(),
                resource_type: arguments
                    .get("resource_type")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                limit,
            },
            principal,
        )
        .await;

    match result {
        Ok(resources) => {
            let content = resources
                .into_iter()
                .map(|hit| {
                    json!({
                        "type": "text",
                        "text": serde_json::to_string(&compact_hit(&hit.resource, &hit.snippet, hit.score)).unwrap_or_else(|_| "{}".into())
                    })
                })
                .collect::<Vec<_>>();
            json_rpc_result(id, json!({ "content": content, "isError": false }))
        }
        Err(error) => tool_error(id, &error.to_string()),
    }
}

fn tool_error(id: Value, message: &str) -> Value {
    json_rpc_result(
        id,
        json!({
            "content": [{ "type": "text", "text": message }],
            "isError": true
        }),
    )
}

fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn write_json(value: &Value) -> Result<(), std::io::Error> {
    println!(
        "{}",
        serde_json::to_string(value).expect("JSON-RPC response is serializable")
    );
    Ok(())
}

async fn search_resources() -> Result<(), Box<dyn std::error::Error>> {
    let text = std::env::args().skip(2).collect::<Vec<_>>().join(" ");
    if text.trim().is_empty() {
        return Err("search text is required".into());
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    let writer = PgResourceWriter::new(pool.clone());
    let resources = writer
        .search(
            ResourceSearchQuery {
                text,
                limit: 20,
                ..Default::default()
            },
            &PrincipalId("local-cli".into()),
        )
        .await?;

    for hit in resources {
        println!(
            "{}",
            serde_json::to_string(&compact_hit(&hit.resource, &hit.snippet, hit.score))?
        );
    }
    pool.close().await;
    Ok(())
}

fn compact_hit(resource: &brain_domain::Resource, snippet: &str, score: f64) -> Value {
    let title = resource
        .data
        .get("title")
        .or_else(|| resource.data.get("name"))
        .or_else(|| resource.data.get("identifier"))
        .and_then(Value::as_str)
        .unwrap_or(&resource.resource_type);
    let identifier = resource.data.get("identifier").and_then(Value::as_str);
    let summary = resource
        .data
        .get("description")
        .and_then(Value::as_str)
        .map(|description| truncate(description, 280))
        .filter(|description| !description.is_empty())
        .unwrap_or_else(|| truncate(snippet, 280));
    let source_url = resource.data.get("url").and_then(Value::as_str);

    json!({
        "id": resource.id.0,
        "type": resource.resource_type,
        "title": title,
        "identifier": identifier,
        "source": {
            "system": format!("{:?}", resource.source_object.system),
            "object_id": resource.source_object.value,
            "url": source_url
        },
        "summary": summary,
        "updated_at": resource.last_observed_at,
        "relevance": score
    })
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut result = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        result.push('…');
    }
    result
}

async fn validate_linear() -> Result<(), Box<dyn std::error::Error>> {
    let connector = LinearConnector::from_env()?;
    let health = connector.validate().await?;
    println!(
        "Linear connector: authenticated={} message={}",
        health.authenticated,
        health.message.as_deref().unwrap_or("none")
    );
    Ok(())
}

async fn sync_linear() -> Result<(), Box<dyn std::error::Error>> {
    let connector = LinearConnector::from_env()?;
    let health = connector.validate().await?;
    if !health.authenticated {
        return Err("Linear authentication failed".into());
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;

    sqlx::query(
        "INSERT INTO connectors (id, source_system, display_name, config_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("linear-main")
    .bind("linear")
    .bind("Linear")
    .bind("{}")
    .bind(runtime_timestamp())
    .execute(&pool)
    .await?;

    let cursor = load_connector_cursor(&pool, "linear-main").await?;
    let repository = PgObservationRepository::new(pool.clone());
    let ingestion = IngestionService::new(repository);
    let resource_writer = PgResourceWriter::new(pool.clone());
    let batch = connector.poll(cursor.clone()).await?;
    let total = batch.events.len();
    let mut inserted = 0;
    let mut duplicates = 0;

    for event in batch.events {
        let observation = observation_for(event);
        match ingestion.ingest(observation.event.clone()).await? {
            brain_storage::AppendResult::Inserted(_) => inserted += 1,
            brain_storage::AppendResult::AlreadyExists(_) => duplicates += 1,
        }
        compile_and_write(&resource_writer, &observation).await?;
    }

    if let Some(next_cursor) = batch.next_cursor.as_ref() {
        save_connector_cursor(&pool, "linear-main", next_cursor).await?;
    }

    println!(
        "Linear sync complete: total={} inserted={} duplicates={} cursor={}",
        total,
        inserted,
        duplicates,
        batch
            .next_cursor
            .as_ref()
            .or(cursor.as_ref())
            .map(|cursor| cursor.0.as_str())
            .unwrap_or("none")
    );
    pool.close().await;
    Ok(())
}

async fn sync_linear_loop() -> Result<(), Box<dyn std::error::Error>> {
    let interval_seconds = std::env::var("LINEAR_SYNC_INTERVAL_SECONDS")
        .unwrap_or_else(|_| "300".into())
        .parse::<u64>()
        .map_err(|_| "LINEAR_SYNC_INTERVAL_SECONDS must be an integer")?;
    if interval_seconds == 0 {
        return Err("LINEAR_SYNC_INTERVAL_SECONDS must be greater than zero".into());
    }

    eprintln!("Linear sync loop started: interval={}s", interval_seconds);
    loop {
        if let Err(error) = sync_linear().await {
            eprintln!("Linear sync failed; retrying after interval: {error}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(interval_seconds)).await;
    }
}

async fn import_claude_artifacts() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(2)
        .ok_or("Claude export path is required")?;
    let connector = ClaudeArtifactConnector::from_path(path);
    let health = connector.validate().await?;
    if !health.authenticated {
        return Err(health
            .message
            .unwrap_or_else(|| "Claude export is not available".into())
            .into());
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    sqlx::query(
        "INSERT INTO connectors (id, source_system, display_name, config_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("claude-artifacts")
    .bind("claude")
    .bind("Claude organization artifacts")
    .bind("{}")
    .bind(runtime_timestamp())
    .execute(&pool)
    .await?;

    let repository = PgObservationRepository::new(pool.clone());
    let ingestion = IngestionService::new(repository);
    let resource_writer = PgResourceWriter::new(pool.clone());
    let batch = connector.backfill(Default::default()).await?;
    let total = batch.events.len();
    let mut inserted = 0;
    let mut duplicates = 0;

    for event in batch.events {
        let observation = observation_for(event);
        match ingestion.ingest(observation.event.clone()).await? {
            brain_storage::AppendResult::Inserted(_) => inserted += 1,
            brain_storage::AppendResult::AlreadyExists(_) => duplicates += 1,
        }
        compile_and_write(&resource_writer, &observation).await?;
    }

    println!(
        "Claude artifact import complete: total={} inserted={} duplicates={}",
        total, inserted, duplicates
    );
    pool.close().await;
    Ok(())
}

async fn import_slack_export() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(2)
        .ok_or("Slack export path is required")?;
    let connector = SlackExportConnector::from_path(path);
    let health = connector.validate().await?;
    if !health.authenticated {
        return Err(health
            .message
            .unwrap_or_else(|| "Slack export is not available".into())
            .into());
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    sqlx::query(
        "INSERT INTO connectors (id, source_system, display_name, config_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("slack-export")
    .bind("slack")
    .bind("Slack workspace export")
    .bind("{}")
    .bind(runtime_timestamp())
    .execute(&pool)
    .await?;

    let repository = PgObservationRepository::new(pool.clone());
    let ingestion = IngestionService::new(repository);
    let resource_writer = PgResourceWriter::new(pool.clone());
    let batch = connector.backfill(Default::default()).await?;
    let total = batch.events.len();
    let mut inserted = 0;
    let mut duplicates = 0;

    for event in batch.events {
        let observation = observation_for(event);
        match ingestion.ingest(observation.event.clone()).await? {
            brain_storage::AppendResult::Inserted(_) => inserted += 1,
            brain_storage::AppendResult::AlreadyExists(_) => duplicates += 1,
        }
        compile_and_write(&resource_writer, &observation).await?;
    }

    println!(
        "Slack export import complete: total={} inserted={} duplicates={}",
        total, inserted, duplicates
    );
    pool.close().await;
    Ok(())
}

async fn sync_slack() -> Result<(), Box<dyn std::error::Error>> {
    let connector = SlackApiConnector::from_env()?;
    let health = connector.validate().await?;
    if !health.authenticated {
        return Err("Slack authentication failed".into());
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    sqlx::query(
        "INSERT INTO connectors (id, source_system, display_name, config_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("slack-api")
    .bind("slack")
    .bind("Slack API")
    .bind("{}")
    .bind(runtime_timestamp())
    .execute(&pool)
    .await?;

    let cursor = load_connector_cursor(&pool, "slack-api").await?;
    let repository = PgObservationRepository::new(pool.clone());
    let ingestion = IngestionService::new(repository);
    let resource_writer = PgResourceWriter::new(pool.clone());
    let batch = connector.poll(cursor.clone()).await?;
    let total = batch.events.len();
    let mut inserted = 0;
    let mut duplicates = 0;

    for event in batch.events {
        let observation = observation_for(event);
        match ingestion.ingest(observation.event.clone()).await? {
            brain_storage::AppendResult::Inserted(_) => inserted += 1,
            brain_storage::AppendResult::AlreadyExists(_) => duplicates += 1,
        }
        compile_and_write(&resource_writer, &observation).await?;
    }

    if let Some(next_cursor) = batch.next_cursor.as_ref() {
        save_connector_cursor(&pool, "slack-api", next_cursor).await?;
    }
    println!(
        "Slack sync complete: total={} inserted={} duplicates={} cursor={}",
        total,
        inserted,
        duplicates,
        batch
            .next_cursor
            .as_ref()
            .or(cursor.as_ref())
            .map(|cursor| cursor.0.as_str())
            .unwrap_or("none")
    );
    pool.close().await;
    Ok(())
}

async fn sync_slack_loop() -> Result<(), Box<dyn std::error::Error>> {
    let interval_seconds = std::env::var("SLACK_SYNC_INTERVAL_SECONDS")
        .unwrap_or_else(|_| "300".into())
        .parse::<u64>()
        .map_err(|_| "SLACK_SYNC_INTERVAL_SECONDS must be an integer")?;
    if interval_seconds == 0 {
        return Err("SLACK_SYNC_INTERVAL_SECONDS must be greater than zero".into());
    }

    eprintln!("Slack sync loop started: interval={}s", interval_seconds);
    loop {
        if let Err(error) = sync_slack().await {
            eprintln!("Slack sync failed; retrying after interval: {error}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(interval_seconds)).await;
    }
}

async fn load_connector_cursor(
    pool: &PgPool,
    connector_id: &str,
) -> Result<Option<Cursor>, Box<dyn std::error::Error>> {
    let row = sqlx::query("SELECT cursor FROM connector_cursors WHERE connector_id = $1")
        .bind(connector_id)
        .fetch_optional(pool)
        .await?;
    let cursor = match row {
        Some(row) => row.try_get::<Option<String>, _>("cursor")?.map(Cursor),
        None => None,
    };
    Ok(cursor)
}

async fn save_connector_cursor(
    pool: &PgPool,
    connector_id: &str,
    cursor: &Cursor,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO connector_cursors (connector_id, cursor, updated_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (connector_id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = EXCLUDED.updated_at",
    )
    .bind(connector_id)
    .bind(&cursor.0)
    .bind(runtime_timestamp())
    .execute(pool)
    .await?;
    Ok(())
}

fn runtime_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}
