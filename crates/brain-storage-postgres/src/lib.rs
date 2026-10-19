//! PostgreSQL implementation of the observation repository.

use async_trait::async_trait;
use brain_domain::{
    ConnectorId, DomainError, DomainResult, Observation, ObservationId, ObservationOperation,
    PrincipalId, Resource, ResourceId, SourceEvent, SourceObjectId, SourcePermission, SourceSystem,
};
use brain_storage::{AppendResult, ObservationFilter, ObservationRepository, ResourceWriter};
use brain_storage::{ResourceSearchHit, ResourceSearchQuery, ResourceSearchRepository};
use sqlx::{Row, postgres::PgPool, postgres::PgPoolOptions};

const INITIAL_MIGRATION: &str = include_str!("../../../migrations/0001_initial.sql");

pub async fn connect(database_url: &str) -> DomainResult<PgPool> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .map_err(sql_error)
}

pub async fn migrate(pool: &PgPool) -> DomainResult<()> {
    sqlx::raw_sql(INITIAL_MIGRATION)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(sql_error)
}

pub async fn connect_and_migrate(database_url: &str) -> DomainResult<PgPool> {
    let pool = connect(database_url).await?;
    migrate(&pool).await?;
    Ok(pool)
}

#[derive(Clone)]
pub struct PgObservationRepository {
    pool: PgPool,
}

impl PgObservationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ObservationRepository for PgObservationRepository {
    async fn append_if_new(&self, observation: Observation) -> DomainResult<AppendResult> {
        let event = &observation.event;
        let permissions = serde_json::to_string(&event.permissions)
            .map_err(|error| DomainError(format!("serialize permissions: {error}")))?;

        let inserted = sqlx::query(
            r#"
            INSERT INTO observations (
                id, connector_id, source_system, source_object_id, operation,
                resource_type, source_version, source_timestamp, observed_at,
                payload_reference, payload_hash, payload_json, permissions_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&observation.id.0)
        .bind(&event.connector_id.0)
        .bind(source_system_name(&event.source_object_id.system))
        .bind(&event.source_object_id.value)
        .bind(operation_name(&event.operation))
        .bind(&event.resource_type)
        .bind(event.source_version.as_deref().unwrap_or(""))
        .bind(event.source_timestamp.as_deref())
        .bind(&event.observed_at)
        .bind(observation.payload_reference.as_deref())
        .bind(&observation.payload_hash)
        .bind(event.payload.as_deref())
        .bind(permissions)
        .fetch_optional(&self.pool)
        .await
        .map_err(sql_error)?;

        if inserted.is_some() {
            Ok(AppendResult::Inserted(observation.id))
        } else {
            Ok(AppendResult::AlreadyExists(observation.id))
        }
    }

    async fn list(&self, filter: ObservationFilter) -> DomainResult<Vec<Observation>> {
        let rows = match filter.connector_id {
            Some(connector_id) => sqlx::query(
                r#"SELECT id, connector_id, source_system, source_object_id, operation,
                          resource_type, source_version, source_timestamp, observed_at,
                          payload_reference, payload_hash, payload_json, permissions_json
                   FROM observations
                   WHERE connector_id = $1
                   ORDER BY observed_at, id"#,
            )
            .bind(connector_id)
            .fetch_all(&self.pool)
            .await
            .map_err(sql_error)?,
            None => sqlx::query(
                r#"SELECT id, connector_id, source_system, source_object_id, operation,
                          resource_type, source_version, source_timestamp, observed_at,
                          payload_reference, payload_hash, payload_json, permissions_json
                   FROM observations
                   ORDER BY observed_at, id"#,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(sql_error)?,
        };

        rows.into_iter().map(observation_from_row).collect()
    }
}

fn observation_from_row(row: sqlx::postgres::PgRow) -> DomainResult<Observation> {
    let permissions_json: String = row.try_get("permissions_json").map_err(sql_error)?;
    let permissions = serde_json::from_str::<Vec<SourcePermission>>(&permissions_json)
        .map_err(|error| DomainError(format!("deserialize permissions: {error}")))?;
    let source_system = source_system_from_name(row.try_get("source_system").map_err(sql_error)?)?;
    let operation = operation_from_name(row.try_get("operation").map_err(sql_error)?)?;
    let source_version: String = row.try_get("source_version").map_err(sql_error)?;

    Ok(Observation {
        id: ObservationId(row.try_get("id").map_err(sql_error)?),
        event: SourceEvent {
            connector_id: ConnectorId(row.try_get("connector_id").map_err(sql_error)?),
            source_object_id: SourceObjectId {
                system: source_system,
                value: row.try_get("source_object_id").map_err(sql_error)?,
            },
            operation,
            resource_type: row.try_get("resource_type").map_err(sql_error)?,
            source_version: (!source_version.is_empty()).then_some(source_version),
            source_timestamp: row.try_get("source_timestamp").map_err(sql_error)?,
            observed_at: row.try_get("observed_at").map_err(sql_error)?,
            payload: row.try_get("payload_json").map_err(sql_error)?,
            permissions,
        },
        payload_reference: row.try_get("payload_reference").map_err(sql_error)?,
        payload_hash: row.try_get("payload_hash").map_err(sql_error)?,
    })
}

#[derive(Clone)]
pub struct PgResourceWriter {
    pool: PgPool,
}

impl PgResourceWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ResourceWriter for PgResourceWriter {
    async fn upsert(&self, resource: Resource) -> DomainResult<()> {
        let data = serde_json::to_string(&resource.data)
            .map_err(|error| DomainError(format!("serialize resource data: {error}")))?;

        sqlx::query(
            r#"
            INSERT INTO resources (
                id, resource_type, source_system, source_object_id, data_json,
                first_observed_at, last_observed_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (source_system, source_object_id) DO UPDATE SET
                id = EXCLUDED.id,
                resource_type = EXCLUDED.resource_type,
                data_json = EXCLUDED.data_json,
                last_observed_at = EXCLUDED.last_observed_at,
                freshness_state = 'current'
            "#,
        )
        .bind(&resource.id.0)
        .bind(&resource.resource_type)
        .bind(source_system_name(&resource.source_object.system))
        .bind(&resource.source_object.value)
        .bind(data)
        .bind(&resource.first_observed_at)
        .bind(&resource.last_observed_at)
        .execute(&self.pool)
        .await
        .map_err(sql_error)?;

        Ok(())
    }
}

#[async_trait]
impl ResourceSearchRepository for PgResourceWriter {
    async fn search(
        &self,
        query: ResourceSearchQuery,
        principal: &PrincipalId,
    ) -> DomainResult<Vec<ResourceSearchHit>> {
        let limit = i64::from(if query.limit == 0 {
            20
        } else {
            query.limit.min(100)
        });
        let rows = sqlx::query(
            r#"
            SELECT r.id, r.resource_type, r.source_system, r.source_object_id,
                   r.data_json, r.first_observed_at, r.last_observed_at,
                   ts_rank(r.search_vector, plainto_tsquery('simple', $1))::double precision AS score,
                   ts_headline('simple', r.data_json, plainto_tsquery('simple', $1),
                       'MaxFragments=2,MaxWords=35,MinWords=8') AS snippet
            FROM resources r
            WHERE (
                r.search_vector @@ plainto_tsquery('simple', $1)
                OR r.data_json ILIKE '%' || $1 || '%'
            )
              AND ($2::text IS NULL OR r.resource_type = $2)
              AND (
                  NOT EXISTS (
                      SELECT 1 FROM source_permissions p
                      WHERE p.source_system = r.source_system
                        AND p.source_resource_key = r.source_object_id
                  )
                  OR EXISTS (
                      SELECT 1 FROM source_permissions p
                      WHERE p.source_system = r.source_system
                        AND p.source_resource_key = r.source_object_id
                        AND p.principal_key = $3
                        AND p.can_read = 1
                  )
              )
            ORDER BY score DESC, r.last_observed_at DESC, r.id
            LIMIT $4
            "#,
        )
        .bind(query.text)
        .bind(query.resource_type)
        .bind(&principal.0)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(sql_error)?;

        rows.into_iter().map(resource_hit_from_row).collect()
    }
}

fn resource_hit_from_row(row: sqlx::postgres::PgRow) -> DomainResult<ResourceSearchHit> {
    let resource = resource_from_row(&row)?;
    let snippet: String = row.try_get("snippet").map_err(sql_error)?;
    let score: f64 = row.try_get("score").map_err(sql_error)?;
    Ok(ResourceSearchHit {
        resource,
        snippet,
        score,
    })
}

fn resource_from_row(row: &sqlx::postgres::PgRow) -> DomainResult<Resource> {
    let source_system = source_system_from_name(row.try_get("source_system").map_err(sql_error)?)?;
    let data_json: String = row.try_get("data_json").map_err(sql_error)?;
    let data = serde_json::from_str(&data_json)
        .map_err(|error| DomainError(format!("deserialize resource data: {error}")))?;

    Ok(Resource {
        id: ResourceId(row.try_get("id").map_err(sql_error)?),
        resource_type: row.try_get("resource_type").map_err(sql_error)?,
        source_object: SourceObjectId {
            system: source_system,
            value: row.try_get("source_object_id").map_err(sql_error)?,
        },
        data,
        first_observed_at: row.try_get("first_observed_at").map_err(sql_error)?,
        last_observed_at: row.try_get("last_observed_at").map_err(sql_error)?,
    })
}

fn source_system_name(system: &SourceSystem) -> &str {
    match system {
        SourceSystem::Linear => "linear",
        SourceSystem::Slack => "slack",
        SourceSystem::Other(_) => "other",
    }
}

fn source_system_from_name(name: String) -> DomainResult<SourceSystem> {
    match name.as_str() {
        "linear" => Ok(SourceSystem::Linear),
        "slack" => Ok(SourceSystem::Slack),
        "other" => Ok(SourceSystem::Other(name)),
        _ => Err(DomainError(format!("unknown source system: {name}"))),
    }
}

fn operation_name(operation: &ObservationOperation) -> &str {
    match operation {
        ObservationOperation::Upsert => "upsert",
        ObservationOperation::Delete => "delete",
    }
}

fn operation_from_name(name: String) -> DomainResult<ObservationOperation> {
    match name.as_str() {
        "upsert" => Ok(ObservationOperation::Upsert),
        "delete" => Ok(ObservationOperation::Delete),
        _ => Err(DomainError(format!(
            "unknown observation operation: {name}"
        ))),
    }
}

fn sql_error(error: impl std::fmt::Display) -> DomainError {
    DomainError(format!("postgres storage error: {error}"))
}
