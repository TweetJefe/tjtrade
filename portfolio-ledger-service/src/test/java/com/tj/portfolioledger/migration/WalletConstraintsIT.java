package com.tj.portfolioledger.migration;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WalletConstraintsIT {

    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private PGSimpleDataSource source;
    private JdbcTemplate jdbc;

    @BeforeAll
    static void startPostgres() {
        String host = System.getenv("DOCKER_HOST");

        if (host != null && !host.isBlank()) {
            assertEquals(URI.create(host), DockerClientFactory.instance().getTransportConfig().getDockerHost());
        }

        DockerClientFactory.instance().client().pingCmd().exec();
        POSTGRES.start();
    }

    @AfterAll
    static void stopPostgres() {
        POSTGRES.stop();
    }

    @BeforeEach
    void createExistingSchema() throws Exception {
        String schema = "wallet_" + UUID.randomUUID().toString().replace("-", "");
        source = new PGSimpleDataSource();
        source.setUrl(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        source.setOptions("-c statement_timeout=15000 -c lock_timeout=12000");
        new JdbcTemplate(source).execute("create schema " + schema);
        source.setCurrentSchema(schema);
        jdbc = new JdbcTemplate(source);
        migrate("user-service", 0);
        migrate("portfolio-ledger-service", 6);
        assertEquals(9, appliedChangesets());
    }

    @Test
    void validUpgradePreservesBalancesUniquenessAndNumericTypes() throws Exception {
        UUID userId = user();
        wallet(userId, "12.34567890", "0.00000001");
        migrate("portfolio-ledger-service", 0);
        migrate("portfolio-ledger-service", 0);

        assertEquals(10, appliedChangesets());
        assertEquals(new BigDecimal("12.34567890"), balance("available"));
        assertEquals(new BigDecimal("0.00000001"), balance("locked"));
        assertEquals(2, jdbc.queryForObject("""
                select count(*) from information_schema.columns
                where table_schema = current_schema() and table_name = 'wallet_balances'
                  and column_name in ('available', 'locked')
                  and numeric_precision = 28 and numeric_scale = 8 and is_nullable = 'NO'
                """, Integer.class));
        assertThrows(DataIntegrityViolationException.class, () -> wallet(userId, "0", "0"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"available", "locked"})
    void negativeExistingBalanceHaltsWithoutMutationAndCanBeRetriedAfterRepair(String column) throws Exception {
        UUID userId = user();
        wallet(userId, "10", "1");
        jdbc.update("update wallet_balances set " + column + " = -1");

        assertThrows(Exception.class, () -> migrate("portfolio-ledger-service", 0));
        assertEquals(new BigDecimal("-1.00000000"), balance(column));
        assertEquals(9, appliedChangesets());
        assertEquals(0, financialConstraints());

        // Explicit fixture repair is not part of the migration.
        jdbc.update("update wallet_balances set " + column + " = 1");
        migrate("portfolio-ledger-service", 0);
        assertEquals(10, appliedChangesets());
    }

    @ParameterizedTest
    @ValueSource(strings = {"available", "locked"})
    void existingNanHaltsWithoutSilentlyConvertingItToMoney(String column) throws Exception {
        wallet(user(), "10", "1");
        jdbc.update("update wallet_balances set " + column + " = 'NaN'::numeric");

        assertThrows(Exception.class, () -> migrate("portfolio-ledger-service", 0));
        assertEquals("NaN", jdbc.queryForObject("select " + column + "::text from wallet_balances", String.class));
        assertEquals(9, appliedChangesets());
        assertEquals(0, financialConstraints());
    }

    @Test
    void orphanWalletHaltsWithoutCreatingUserOrDeletingWallet() throws Exception {
        wallet(UUID.randomUUID(), "10", "0");

        assertThrows(Exception.class, () -> migrate("portfolio-ledger-service", 0));
        assertEquals(1, jdbc.queryForObject("select count(*) from wallet_balances", Integer.class));
        assertEquals(0, jdbc.queryForObject("select count(*) from users", Integer.class));
        assertEquals(9, appliedChangesets());
    }

    @Test
    void unexpectedNumericPrecisionHaltsInsteadOfConvertingExistingData() {
        jdbc.execute("alter table wallet_balances alter column available type numeric(27,8)");

        assertThrows(Exception.class, () -> migrate("portfolio-ledger-service", 0));
        assertEquals(9, appliedChangesets());
        assertEquals(27, jdbc.queryForObject("""
                select numeric_precision from information_schema.columns
                where table_schema = current_schema() and table_name = 'wallet_balances' and column_name = 'available'
                """, Integer.class));
    }

    @Test
    void databaseRejectsNegativeNanNullOverflowAndOrphanWrites() throws Exception {
        wallet(user(), "0", "0");
        migrate("portfolio-ledger-service", 0);

        for (String column : new String[]{"available", "locked"}) {
            for (String value : new String[]{"-1", "'NaN'::numeric", "NULL", "100000000000000000000"}) {
                assertThrows(DataIntegrityViolationException.class,
                        () -> jdbc.update("update wallet_balances set " + column + " = " + value));
            }

            jdbc.update("update wallet_balances set " + column + " = 99999999999999999999.99999999");
            assertEquals(new BigDecimal("99999999999999999999.99999999"), balance(column));
            jdbc.update("update wallet_balances set " + column + " = 0");
        }

        assertThrows(DataIntegrityViolationException.class, () -> wallet(UUID.randomUUID(), "1", "0"));
    }

    @Test
    void deletionOfWalletOwnerIsRestrictedAndStatusChangePreservesWallet() throws Exception {
        UUID userId = user();
        wallet(userId, "10", "2");
        migrate("portfolio-ledger-service", 0);

        assertThrows(DataIntegrityViolationException.class, () -> jdbc.update("delete from users where id = ?", userId));
        jdbc.update("update users set status = 'CLOSED' where id = ?", userId);
        assertEquals(new BigDecimal("10.00000000"), balance("available"));
        assertEquals(new BigDecimal("2.00000000"), balance("locked"));
    }

    @Test
    void deletionOfUserWithUnreservedOrderIsRestrictedAndOrderSurvives() throws Exception {
        UUID userId = user();
        UUID orderId = UUID.randomUUID();
        jdbc.update("""
                insert into orders (id, user_id, symbol, side, type, price, quantity)
                values (?, ?, 'BTC_USDT', 'BUY', 'LIMIT', 100, 1)
                """, orderId, userId);
        migrate("portfolio-ledger-service", 0);

        assertThrows(DataIntegrityViolationException.class, () -> jdbc.update("delete from users where id = ?", userId));
        assertEquals(1, jdbc.queryForObject("select count(*) from orders where id = ?", Integer.class, orderId));
        assertEquals("r", jdbc.queryForObject("""
                select confdeltype::text from pg_constraint
                where conrelid = 'orders'::regclass and conname = 'orders_user_id_fkey'
                """, String.class));
        UUID emptyUserId = user();
        assertEquals(1, jdbc.update("delete from users where id = ?", emptyUserId));
    }

    private UUID user() {
        UUID userId = UUID.randomUUID();
        jdbc.update("insert into users (id, email, password_hash) values (?, ?, 'fixture')", userId, userId + "@test.local");

        return userId;
    }

    private void wallet(UUID userId, String available, String locked) {
        jdbc.update("insert into wallet_balances (user_id, currency, available, locked) values (?, 'USDT', ?, ?)",
                userId, new BigDecimal(available), new BigDecimal(locked));
    }

    private BigDecimal balance(String column) {
        return jdbc.queryForObject("select " + column + " from wallet_balances", BigDecimal.class);
    }

    private int appliedChangesets() {
        return jdbc.queryForObject("select count(*) from databasechangelog", Integer.class);
    }

    private int financialConstraints() {
        return jdbc.queryForObject("""
                select count(*) from pg_constraint where conrelid = 'wallet_balances'::regclass
                  and conname in ('ck_wallet_available_range', 'ck_wallet_locked_range', 'fk_wallet_balances_user')
                """, Integer.class);
    }

    private void migrate(String module, int count) throws Exception {
        try (var connection = source.getConnection();
             var resources = new DirectoryResourceAccessor(ROOT.resolve(module).resolve("src/main/resources"))) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

            try (var migration = new Liquibase("db/changelog/db.changelog-master.xml", resources, database)) {
                if (count == 0) {
                    migration.update(new Contexts(), new LabelExpression());
                } else {
                    migration.update(count, new Contexts(), new LabelExpression());
                }

                migration.validate();
            }
        }
    }
}
