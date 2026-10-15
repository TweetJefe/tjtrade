--liquibase formatted sql

--changeset tjtrade:transactional_outbox
CREATE TABLE outbox_sequence (
    stream VARCHAR(100) NOT NULL,
    aggregate_key VARCHAR(255) NOT NULL,
    last_sequence BIGINT NOT NULL CHECK (last_sequence > 0),
    PRIMARY KEY (stream, aggregate_key)
);

CREATE TABLE outbox_event (
    event_id UUID PRIMARY KEY,
    stream VARCHAR(100) NOT NULL,
    aggregate_key VARCHAR(255) NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_type VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    correlation_id UUID NOT NULL,
    payload JSONB NOT NULL,
    payload_hash VARCHAR(64) NOT NULL CHECK (length(payload_hash) = 64),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (stream, aggregate_key, sequence)
);

CREATE INDEX idx_outbox_event_correlation_id ON outbox_event(correlation_id);
CREATE INDEX idx_outbox_event_pending ON outbox_event(stream, aggregate_key, sequence)
    WHERE published_at IS NULL;
