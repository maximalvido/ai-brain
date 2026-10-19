# Knowledge Model

Status: Proposed

## Design goals

The model must distinguish what a source said from what AI Brain derived. It must support provenance, temporal state, uncertainty, permissions, and rebuildable indexes.

## Core concepts

### Source observation

An immutable record of a source payload or deletion notice.

```text
Observation
- id
- connector_id
- source_system
- source_object_id
- source_version
- operation: upsert | delete
- payload_reference
- source_created_at
- source_updated_at
- observed_at
- source_permissions
```

### Resource

A normalized representation of a source object, such as a Linear issue, Slack message, project, channel, team, or person.

```text
Resource
- id
- type
- normalized_data
- source_references
- current_state
- first_observed_at
- last_observed_at
- freshness_state
```

### Entity

A stable identity used to connect resources across systems. Entity resolution may be uncertain and must retain candidate matches and confidence.

Examples: person, team, project, issue, channel, repository, customer.

### Assertion

A claim extracted or normalized from one or more resources.

```text
Assertion
- subject
- predicate
- object_or_value
- valid_from
- valid_until
- status: proposed | verified | rejected | superseded
- confidence
- provenance
- permission_scope
- compiler_version
```

Generated assertions are not authoritative merely because they are stored. Human verification and source authority must remain explicit.

### Relation

A typed connection between entities or resources, such as `blocks`, `owned_by`, `discusses`, `belongs_to`, or `related_to`. Relations require provenance and may have temporal validity.

### Projection

A derived representation optimized for retrieval: text, chunk, embedding, summary, facet, or relationship index. Projections are disposable and rebuildable.

## Provenance

Every assertion, relation, summary, and retrieval result should be traceable to one or more observations or resources.

At minimum, provenance records:

- source system;
- source object and URL;
- observation ID;
- compiler stage and version;
- model and prompt version when applicable;
- generation time;
- supporting evidence;
- freshness state.

## Freshness and temporal knowledge

Store separately:

- source-created time;
- source-updated time;
- observed time;
- derived time;
- validity interval when the fact describes a changing condition.

An index or summary may be stale even when the source resource is current. Freshness must therefore be tracked per projection.

## Contradictions and supersession

Do not overwrite conflicting assertions without recording the conflict. Store competing claims with their evidence, timestamps, confidence, and status. A later or more authoritative source may supersede an older claim, but the history remains inspectable.

## Initial model constraints

The first release should focus on resources, entities, relations, observations, permissions, and projections. Complex ontology management and broad automatic fact extraction are deferred until the retrieval loop demonstrates a need for them.
