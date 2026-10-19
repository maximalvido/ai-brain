use brain_domain::{
    ConnectorId, Observation, ObservationId, ObservationOperation, SourceEvent, SourceObjectId,
    SourceSystem,
};
use brain_storage::{AppendResult, ObservationFilter, ObservationRepository};
use brain_storage_postgres::{PgObservationRepository, connect_and_migrate};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    tokio::runtime::Runtime::new()?.block_on(async {
        let database_url = std::env::var("DATABASE_URL")?;
        let pool = connect_and_migrate(&database_url).await?;

        sqlx::query(
        "INSERT INTO connectors (id, source_system, display_name, config_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind("smoke-linear")
    .bind("linear")
    .bind("Smoke Linear")
    .bind("{}")
    .bind("2026-10-17T00:00:00Z")
        .execute(&pool)
        .await?;

    sqlx::query("DELETE FROM observations WHERE id = $1")
        .bind("smoke-observation-1")
        .execute(&pool)
        .await?;

        let repository = PgObservationRepository::new(pool.clone());
        let observation = Observation {
        id: ObservationId("smoke-observation-1".into()),
        event: SourceEvent {
            connector_id: ConnectorId("smoke-linear".into()),
            source_object_id: SourceObjectId {
                system: SourceSystem::Linear,
                value: "smoke-issue-1".into(),
            },
            resource_type: "issue".into(),
            operation: ObservationOperation::Upsert,
            source_version: Some("1".into()),
            source_timestamp: None,
            observed_at: "2026-10-17T00:00:00Z".into(),
            payload: Some("{\"title\":\"Smoke test\"}".into()),
            permissions: vec![],
        },
        payload_reference: None,
        payload_hash: "smoke-hash".into(),
        };

        let first = repository.append_if_new(observation.clone()).await?;
        let second = repository.append_if_new(observation).await?;
        let observations = repository
            .list(ObservationFilter {
                connector_id: Some("smoke-linear".into()),
                after: None,
            })
            .await?;

        assert!(matches!(first, AppendResult::Inserted(_)));
        assert!(matches!(second, AppendResult::AlreadyExists(_)));
        assert_eq!(observations.len(), 1);

        println!("PostgreSQL smoke test passed: migration, insert, dedupe, and read");
        pool.close().await;
        Ok(())
    })
}
