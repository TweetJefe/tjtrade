--liquibase formatted sql

--changeset tjtrade:outbox_publish_cursor
CREATE TABLE outbox_publish_cursor (
    stream VARCHAR(100) NOT NULL,
    aggregate_key VARCHAR(255) NOT NULL,
    last_published_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_published_sequence >= 0),
    PRIMARY KEY (stream, aggregate_key)
);

-- Existing streams start at zero; allocation is not proof of publication.
INSERT INTO outbox_publish_cursor (stream, aggregate_key, last_published_sequence)
SELECT stream, aggregate_key, 0 FROM outbox_sequence;
