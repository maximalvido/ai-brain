//! Durable-ingestion orchestration primitives.

use brain_domain::{Observation, ObservationId, SourceEvent};
use brain_storage::{AppendResult, ObservationRepository};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct IngestionService<R> {
    repository: R,
}

impl<R> IngestionService<R>
where
    R: ObservationRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn ingest(&self, event: SourceEvent) -> brain_domain::DomainResult<AppendResult> {
        let observation = observation_for(event);

        self.repository.append_if_new(observation).await
    }
}

pub fn observation_for(event: SourceEvent) -> Observation {
    let payload_hash = stable_payload_hash(&event);
    Observation {
        id: ObservationId(observation_key(&event, &payload_hash)),
        event,
        payload_reference: None,
        payload_hash,
    }
}

fn observation_key(event: &SourceEvent, payload_hash: &str) -> String {
    format!(
        "{}:{}:{}:{}:{:?}:{}",
        event.connector_id.0,
        event.resource_type,
        event.source_object_id.value,
        event.source_version.as_deref().unwrap_or("unversioned"),
        event.operation,
        payload_hash
    )
}

fn stable_payload_hash(event: &SourceEvent) -> String {
    let mut hasher = DefaultHasher::new();
    event.payload.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_domain::{ConnectorId, ObservationOperation, SourceObjectId, SourceSystem};
    use brain_storage::InMemoryObservationRepository;

    fn event() -> SourceEvent {
        SourceEvent {
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
            payload: Some("{\"state\":\"started\"}".into()),
            permissions: vec![],
        }
    }

    #[test]
    fn duplicate_events_are_idempotent() {
        let repository = InMemoryObservationRepository::default();
        let service = IngestionService::new(repository.clone());

        let future = async {
            let first = service.ingest(event()).await.expect("first ingest");
            let second = service.ingest(event()).await.expect("duplicate ingest");

            assert!(matches!(first, AppendResult::Inserted(_)));
            assert!(matches!(second, AppendResult::AlreadyExists(_)));
            assert_eq!(repository.len().expect("repository length"), 1);
        };
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        assert!(matches!(
            std::future::Future::poll(future.as_mut(), &mut context),
            std::task::Poll::Ready(())
        ));
    }
}
