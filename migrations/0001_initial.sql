-- Initial logical schema for the self-hosted single-installation Brain.
-- The concrete migration runner and database adapter are added after the
-- domain boundaries are reviewed.

CREATE TABLE IF NOT EXISTS connectors (
    id TEXT PRIMARY KEY,
    source_system TEXT NOT NULL,
    display_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_cursors (
    connector_id TEXT PRIMARY KEY REFERENCES connectors(id),
    cursor TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    id TEXT PRIMARY KEY,
    connector_id TEXT NOT NULL REFERENCES connectors(id),
    source_system TEXT NOT NULL,
    source_object_id TEXT NOT NULL,
    resource_type TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL,
    source_version TEXT NOT NULL DEFAULT '',
    source_timestamp TEXT,
    observed_at TEXT NOT NULL,
    payload_reference TEXT,
    payload_hash TEXT NOT NULL,
    payload_json TEXT,
    permissions_json TEXT NOT NULL DEFAULT '[]',
    UNIQUE (connector_id, source_object_id, source_version, operation, payload_hash)
);

ALTER TABLE observations ADD COLUMN IF NOT EXISTS resource_type TEXT NOT NULL DEFAULT '';
ALTER TABLE observations DROP CONSTRAINT IF EXISTS observations_connector_id_source_object_id_source_version_o_key;
CREATE UNIQUE INDEX IF NOT EXISTS observations_identity_idx
    ON observations (connector_id, source_object_id, resource_type, source_version, operation, payload_hash);

CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_object_id TEXT NOT NULL,
    data_json TEXT NOT NULL,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    freshness_state TEXT NOT NULL DEFAULT 'current',
    search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', data_json)) STORED,
    UNIQUE (source_system, source_object_id)
);

ALTER TABLE resources ADD COLUMN IF NOT EXISTS search_vector TSVECTOR
    GENERATED ALWAYS AS (to_tsvector('simple', data_json)) STORED;
CREATE INDEX IF NOT EXISTS resources_search_vector_idx ON resources USING GIN (search_vector);

CREATE TABLE IF NOT EXISTS resource_source_refs (
    resource_id TEXT NOT NULL REFERENCES resources(id),
    source_system TEXT NOT NULL,
    source_object_id TEXT NOT NULL,
    source_url TEXT,
    PRIMARY KEY (resource_id, source_system, source_object_id)
);

CREATE TABLE IF NOT EXISTS source_permissions (
    source_system TEXT NOT NULL,
    source_resource_key TEXT NOT NULL,
    principal_key TEXT NOT NULL,
    can_read INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (source_system, source_resource_key, principal_key)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    input_key TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
