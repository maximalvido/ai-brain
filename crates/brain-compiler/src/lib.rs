//! Deterministic conversion from source observations into canonical resources.
//!
//! This first compiler intentionally does not call an LLM. It preserves the
//! source payload and adds stable identity and provenance, giving retrieval a
//! dependable base before semantic enrichment is introduced.

use brain_domain::{Observation, ObservationOperation, Resource, ResourceId, SourceSystem};
use brain_storage::ResourceWriter;

pub fn compile(observation: &Observation) -> brain_domain::DomainResult<Option<Resource>> {
    if observation.event.operation == ObservationOperation::Delete {
        return Ok(None);
    }

    let payload = observation.event.payload.as_deref().unwrap_or("null");
    let data = serde_json::from_str(payload).map_err(|error| {
        brain_domain::DomainError(format!(
            "parse {} payload: {error}",
            observation.event.resource_type
        ))
    })?;

    Ok(Some(Resource {
        id: ResourceId(resource_id(
            &observation.event.source_object_id.system,
            &observation.event.source_object_id.value,
        )),
        resource_type: observation.event.resource_type.clone(),
        source_object: observation.event.source_object_id.clone(),
        data,
        first_observed_at: observation.event.observed_at.clone(),
        last_observed_at: observation.event.observed_at.clone(),
    }))
}

pub async fn compile_and_write<W: ResourceWriter>(
    writer: &W,
    observation: &Observation,
) -> brain_domain::DomainResult<bool> {
    let Some(resource) = compile(observation)? else {
        return Ok(false);
    };
    writer.upsert(resource).await?;
    Ok(true)
}

fn resource_id(system: &SourceSystem, source_object_id: &str) -> String {
    let system = match system {
        SourceSystem::Linear => "linear",
        SourceSystem::Slack => "slack",
        SourceSystem::Claude => "claude",
        SourceSystem::Other(name) => name,
    };
    format!("resource:{system}:{source_object_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_domain::{ConnectorId, ObservationId, SourceEvent, SourceObjectId};

    #[test]
    fn compiles_linear_observation_with_stable_identity() {
        let observation = Observation {
            id: ObservationId("observation-1".into()),
            event: SourceEvent {
                connector_id: ConnectorId("linear-main".into()),
                source_object_id: SourceObjectId {
                    system: SourceSystem::Linear,
                    value: "issue-1".into(),
                },
                resource_type: "issue".into(),
                operation: ObservationOperation::Upsert,
                source_version: Some("v1".into()),
                source_timestamp: None,
                observed_at: "2026-10-17T00:00:00Z".into(),
                payload: Some(r#"{"title":"Ship it"}"#.into()),
                permissions: vec![],
            },
            payload_reference: None,
            payload_hash: "hash".into(),
        };

        let resource = compile(&observation).expect("compile").expect("resource");
        assert_eq!(resource.id.0, "resource:linear:issue-1");
        assert_eq!(resource.resource_type, "issue");
        assert_eq!(resource.data["title"], "Ship it");
    }
}
