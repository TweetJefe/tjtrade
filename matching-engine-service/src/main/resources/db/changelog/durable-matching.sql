--liquibase formatted sql

--changeset tjtrade:durable_matching_commands
CREATE TABLE matching_market (
    symbol VARCHAR(20) PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0)
);

CREATE TABLE matching_order (
    order_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    symbol VARCHAR(20) NOT NULL REFERENCES matching_market(symbol) ON DELETE RESTRICT,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price NUMERIC(18, 8) NOT NULL CHECK (price > 0),
    quantity NUMERIC(18, 8) NOT NULL CHECK (quantity > 0),
    remaining_quantity NUMERIC(18, 8) NOT NULL CHECK (remaining_quantity >= 0 AND remaining_quantity <= quantity),
    accepted_sequence BIGINT NOT NULL CHECK (accepted_sequence > 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (symbol, accepted_sequence)
);

CREATE INDEX idx_matching_order_live ON matching_order(symbol, side, price, accepted_sequence)
    WHERE remaining_quantity > 0;

CREATE TABLE matching_inbox (
    consumer VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    payload_hash VARCHAR(64) NOT NULL CHECK (length(payload_hash) = 64),
    symbol VARCHAR(20) NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    order_id UUID NOT NULL REFERENCES matching_order(order_id) ON DELETE RESTRICT,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer, event_id),
    UNIQUE (consumer, symbol, sequence),
    UNIQUE (consumer, order_id)
);

CREATE TABLE matching_result_outbox (
    event_id UUID PRIMARY KEY,
    command_event_id UUID NOT NULL UNIQUE,
    symbol VARCHAR(20) NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (symbol, sequence)
);
