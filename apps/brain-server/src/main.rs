use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use brain_compiler::compile_and_write;
use brain_connectors::{BackfillRequest, Connector};
use brain_domain::PrincipalId;
use brain_ingestion::{IngestionService, observation_for};
use brain_storage::{ResourceSearchQuery, ResourceSearchRepository};
use brain_storage_postgres::{PgObservationRepository, PgResourceWriter, connect_and_migrate};
use connector_linear::LinearConnector;
use serde_json::{Value, json};
use std::io::BufRead;
use std::net::SocketAddr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        match std::env::args().nth(1).as_deref() {
            Some("sync") => sync_linear().await,
            Some("validate-linear") => validate_linear().await,
            Some("search") => search_resources().await,
            Some("mcp") => mcp_server().await,
            Some("serve-http") => serve_http().await,
            _ => {
                println!("Usage: brain-server <sync|validate-linear|search <text>|mcp|serve-http>");
                Ok(())
            }
        }
    })
}

#[derive(Clone)]
struct HttpState {
    repository: PgResourceWriter,
    access_key: String,
}

async fn serve_http() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = connect_and_migrate(&database_url).await?;
    let state = HttpState {
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
            .with_state(state),
    )
    .await?;
    Ok(())
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

    let repository = PgObservationRepository::new(pool.clone());
    let ingestion = IngestionService::new(repository);
    let resource_writer = PgResourceWriter::new(pool.clone());
    let batch = connector.backfill(BackfillRequest::default()).await?;
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
        "Linear sync complete: total={} inserted={} duplicates={}",
        total, inserted, duplicates
    );
    pool.close().await;
    Ok(())
}

fn runtime_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}
