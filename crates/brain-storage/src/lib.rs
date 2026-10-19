//! Storage contracts. Concrete database implementations are intentionally
//! deferred until these domain contracts and migrations are reviewed.

use async_trait::async_trait;
use brain_domain::{DomainResult, Observation, ObservationId, PrincipalId, Resource, ResourceId};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObservationFilter {
    pub connector_id: Option<String>,
    pub after: Option<ObservationId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceSearchQuery {
    pub text: String,
    pub resource_type: Option<String>,
    pub limit: u32,
}

#[derive(Clone, Debug)]
pub struct ResourceSearchHit {
    pub resource: Resource,
    pub snippet: String,
    pub score: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendResult {
    Inserted(ObservationId),
    AlreadyExists(ObservationId),
}

#[async_trait]
pub trait ObservationRepository: Send + Sync {
    async fn append_if_new(&self, observation: Observation) -> DomainResult<AppendResult>;
    async fn list(&self, filter: ObservationFilter) -> DomainResult<Vec<Observation>>;
}

#[async_trait]
pub trait ResourceWriter: Send + Sync {
    async fn upsert(&self, resource: Resource) -> DomainResult<()>;
}

#[async_trait]
pub trait ResourceRepository: ResourceWriter {
    async fn get(&self, id: ResourceId, principal: &PrincipalId) -> DomainResult<Option<Resource>>;
}

#[async_trait]
pub trait ResourceSearchRepository: Send + Sync {
    async fn search(
        &self,
        query: ResourceSearchQuery,
        principal: &PrincipalId,
    ) -> DomainResult<Vec<ResourceSearchHit>>;
}

#[derive(Clone, Default)]
pub struct InMemoryObservationRepository {
    observations: Arc<Mutex<BTreeMap<String, Observation>>>,
}

impl InMemoryObservationRepository {
    pub fn len(&self) -> DomainResult<usize> {
        self.observations
            .lock()
            .map(|observations| observations.len())
            .map_err(|_| brain_domain::DomainError("observation lock poisoned".into()))
    }

    pub fn is_empty(&self) -> DomainResult<bool> {
        Ok(self.len()? == 0)
    }
}

#[async_trait]
impl ObservationRepository for InMemoryObservationRepository {
    async fn append_if_new(&self, observation: Observation) -> DomainResult<AppendResult> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| brain_domain::DomainError("observation lock poisoned".into()))?;
        let key = observation.id.0.clone();

        if observations.contains_key(&key) {
            return Ok(AppendResult::AlreadyExists(observation.id));
        }

        let id = observation.id.clone();
        observations.insert(key, observation);
        Ok(AppendResult::Inserted(id))
    }

    async fn list(&self, _filter: ObservationFilter) -> DomainResult<Vec<Observation>> {
        self.observations
            .lock()
            .map(|observations| observations.values().cloned().collect())
            .map_err(|_| brain_domain::DomainError("observation lock poisoned".into()))
    }
}
