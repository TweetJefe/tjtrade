--liquibase formatted sql

--changeset tjtrade:wallet_financial_constraints runInTransaction:true
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:0 SELECT count(*) FROM wallet_balances WHERE NOT (available >= 0 AND available < 100000000000000000000 AND locked >= 0 AND locked < 100000000000000000000)
--precondition-sql-check expectedResult:0 SELECT count(*) FROM wallet_balances w LEFT JOIN users u ON u.id = w.user_id WHERE u.id IS NULL
--precondition-sql-check expectedResult:2 SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'wallet_balances' AND column_name IN ('available', 'locked') AND data_type = 'numeric' AND numeric_precision = 28 AND numeric_scale = 8 AND is_nullable = 'NO'

-- Validate existing data without rewriting balances or changing numeric precision.
ALTER TABLE wallet_balances
    ADD CONSTRAINT ck_wallet_available_range
        CHECK (available >= 0 AND available < 100000000000000000000),
    ADD CONSTRAINT ck_wallet_locked_range
        CHECK (locked >= 0 AND locked < 100000000000000000000);

-- Financial references prevent profile deletion from removing or orphaning history.
ALTER TABLE wallet_balances
    ADD CONSTRAINT fk_wallet_balances_user
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT;

ALTER TABLE orders DROP CONSTRAINT orders_user_id_fkey;
ALTER TABLE orders
    ADD CONSTRAINT orders_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT;
