--liquibase formatted sql

--changeset tjtrade:fund_reservation
CREATE TABLE fund_reservation (
    order_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    asset VARCHAR(20) NOT NULL,
    amount NUMERIC(28, 8) NOT NULL CHECK (amount > 0),
    remaining_amount NUMERIC(28, 8) NOT NULL
        CHECK (remaining_amount >= 0 AND remaining_amount <= amount),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE', 'CONSUMED', 'RELEASED')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fund_reservation_user_id
    ON fund_reservation(user_id);