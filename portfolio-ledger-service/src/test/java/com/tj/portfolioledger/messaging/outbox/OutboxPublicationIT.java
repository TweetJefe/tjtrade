package com.tj.portfolioledger.messaging.outbox;

import com.tj.portfolioledger.repository.OutboxPublishCursorRepository;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OutboxPublicationIT {

    private static final String STREAM = "matching.commands";
    private static final String EXISTING_MARKET = "EXISTING_USDT";
    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static AnnotationConfigApplicationContext context;
    private static JdbcTemplate jdbc;
    private static TransactionTemplate transaction;
    private static OutboxPublishCursorRepository repository;
    private static OutboxPublicationCoordinator coordinator;

    @BeforeAll
    static void startDatabaseAndCoordinator() throws Exception {
        String configuredHost = System.getenv("DOCKER_HOST");

        if (configuredHost != null && !configuredHost.isBlank()) {
            assertEquals(URI.create(configuredHost), DockerClientFactory.instance().getTransportConfig().getDockerHost(),
                    "Configured DOCKER_HOST is unavailable; refusing to use another Docker engine");
        }

        DockerClientFactory.instance().client().pingCmd().exec();

        try {
            POSTGRES.start();

            var source = new PGSimpleDataSource();
            source.setUrl(POSTGRES.getJdbcUrl());
            source.setUser(POSTGRES.getUsername());
            source.setPassword(POSTGRES.getPassword());
            source.setOptions("-c statement_timeout=15000 -c lock_timeout=12000");
            jdbc = new JdbcTemplate(source);

            migrate(source, "user-service", false);
            migrate(source, "portfolio-ledger-service", true);
            jdbc.update("insert into outbox_sequence (stream, aggregate_key, last_sequence) values (?, ?, ?)",
                    STREAM, EXISTING_MARKET, 3L);
            migrate(source, "portfolio-ledger-service", false);
            migrate(source, "portfolio-ledger-service", false);

            context = new AnnotationConfigApplicationContext();
            context.registerBean(DataSource.class, () -> source);
            context.registerBean(DSLContext.class, () -> DSL.using(
                    new TransactionAwareDataSourceProxy(source), SQLDialect.POSTGRES
            ));
            context.register(TransactionConfiguration.class, OutboxPublishCursorRepository.class,
                    OutboxPublicationCoordinator.class);
            context.refresh();

            repository = context.getBean(OutboxPublishCursorRepository.class);
            coordinator = context.getBean(OutboxPublicationCoordinator.class);
            transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
            transaction.setTimeout(10);
        } catch (Exception | AssertionError failure) {
            stopDatabaseAndCoordinator();
            throw failure;
        }
    }

    private static void migrate(DataSource source, String module, boolean beforeCursor) throws Exception {
        try (var connection = source.getConnection();
             var resources = new DirectoryResourceAccessor(ROOT.resolve(module).resolve("src/main/resources"))) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

            try (var migration = new Liquibase("db/changelog/db.changelog-master.xml", resources, database)) {
                if (beforeCursor) {
                    // The five existing ledger changesets precede the cursor migration.
                    migration.update(5, new Contexts(), new LabelExpression());
                } else {
                    migration.update(new Contexts(), new LabelExpression());
                }

                migration.validate();
            }
        }
    }

    @AfterAll
    static void stopDatabaseAndCoordinator() {
        try {
            if (context != null) {
                context.close();
            }
        } finally {
            POSTGRES.stop();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TransactionConfiguration {

        @Bean
        PlatformTransactionManager transactionManager(DataSource source) {
            var manager = new DataSourceTransactionManager(source);
            manager.setValidateExistingTransaction(true);

            return manager;
        }
    }

    @Test
    void migrationInitializesExistingStreamsAtZero() throws Exception {
        assertEquals(3L, jdbc.queryForObject(
                "select last_sequence from outbox_sequence where stream = ? and aggregate_key = ?",
                Long.class, STREAM, EXISTING_MARKET
        ));
        assertTrue(coordinator.tryRun(STREAM, EXISTING_MARKET,
                cursor -> assertEquals(0L, cursor.lastPublishedSequence())));
        assertEquals(9, jdbc.queryForObject("select count(*) from databasechangelog", Integer.class));
    }

    @Test
    void initializationDoesNotResetExistingPosition() throws Exception {
        String market = newMarket();
        jdbc.update("update outbox_publish_cursor set last_published_sequence = 7 where stream = ? and aggregate_key = ?",
                STREAM, market);

        transaction.executeWithoutResult(status -> repository.initializeIfAbsent(STREAM, market));

        assertTrue(coordinator.tryRun(STREAM, market, cursor -> {
            assertEquals(STREAM, cursor.stream());
            assertEquals(market, cursor.aggregateKey());
            assertEquals(7L, cursor.lastPublishedSequence());
        }));
    }

    @Test
    void cursorCreationRollsBackWithItsTransaction() {
        String market = UUID.randomUUID().toString();

        transaction.executeWithoutResult(status -> {
            repository.initializeIfAbsent(STREAM, market);
            status.setRollbackOnly();
        });

        assertEquals(0, jdbc.queryForObject(
                "select count(*) from outbox_publish_cursor where stream = ? and aggregate_key = ?",
                Integer.class, STREAM, market
        ));
    }

    @Test
    void repositoryRejectsCallsWithoutTransaction() {
        assertThrows(IllegalTransactionStateException.class,
                () -> repository.initializeIfAbsent(STREAM, "NO_TRANSACTION"));
        assertThrows(IllegalTransactionStateException.class,
                () -> repository.tryLock(STREAM, "NO_TRANSACTION"));
    }

    @Test
    void competingPublisherSkipsLockedMarketAndCanAcquireAfterCommit() throws Exception {
        String market = newMarket();
        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var contenderRan = new AtomicBoolean();

        try (var executor = Executors.newFixedThreadPool(2)) {
            var owner = executor.submit(() -> coordinator.tryRun(STREAM, market, cursor -> {
                locked.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }));

            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                var contender = executor.submit(() -> coordinator.tryRun(STREAM, market,
                        cursor -> contenderRan.set(true)));
                assertFalse(contender.get(3, TimeUnit.SECONDS));
                assertFalse(contenderRan.get());
            } finally {
                release.countDown();
            }

            assertTrue(owner.get(5, TimeUnit.SECONDS));
        }

        assertTrue(coordinator.tryRun(STREAM, market, cursor -> contenderRan.set(true)));
        assertTrue(contenderRan.get());
    }

    @Test
    void differentMarketsCanRunConcurrently() throws Exception {
        String firstMarket = newMarket();
        String secondMarket = newMarket();
        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var owner = executor.submit(() -> coordinator.tryRun(STREAM, firstMarket, cursor -> {
                locked.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }));

            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                var other = executor.submit(() -> coordinator.tryRun(STREAM, secondMarket,
                        cursor -> assertEquals(secondMarket, cursor.aggregateKey())));
                assertTrue(other.get(3, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }

            assertTrue(owner.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void checkedFailureRollsBackWorkAndReleasesOwnership() throws Exception {
        String market = newMarket();

        assertThrows(IOException.class, () -> coordinator.tryRun(STREAM, market, cursor -> {
            jdbc.update("update outbox_publish_cursor set last_published_sequence = 1 where stream = ? and aggregate_key = ?",
                    STREAM, market);
            throw new IOException("Simulated publication failure");
        }));

        assertTrue(coordinator.tryRun(STREAM, market, cursor -> assertEquals(0L, cursor.lastPublishedSequence())));
    }

    @Test
    void publicationTransactionIsIndependentFromCaller() {
        String market = newMarket();

        transaction.executeWithoutResult(status -> {
            try {
                assertTrue(coordinator.tryRun(STREAM, market, cursor -> jdbc.update(
                        "update outbox_publish_cursor set last_published_sequence = 1 where stream = ? and aggregate_key = ?",
                        STREAM, market
                )));
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }

            status.setRollbackOnly();
        });

        assertEquals(1L, position(market));
    }

    @Test
    void invalidOrAbsentStreamDoesNotRunWork() throws Exception {
        var invoked = new AtomicBoolean();
        OutboxPublication.Work work = cursor -> invoked.set(true);

        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun(null, "BTC_USDT", work));
        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun(" ", "BTC_USDT", work));
        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun("x".repeat(101), "BTC_USDT", work));
        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun(STREAM, " BTC_USDT", work));
        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun(STREAM, "x".repeat(256), work));
        assertThrows(IllegalArgumentException.class, () -> coordinator.tryRun(STREAM, "BTC_USDT", null));
        assertFalse(coordinator.tryRun(STREAM, UUID.randomUUID().toString(), work));
        assertFalse(invoked.get());
    }

    @Test
    void lostDatabaseSessionAllowsTakeoverButCannotCommitOldWork() throws Exception {
        String market = newMarket();
        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var backend = new AtomicInteger();

        try (var executor = Executors.newSingleThreadExecutor()) {
            var oldOwner = executor.submit(() -> coordinator.tryRun(STREAM, market, cursor -> {
                backend.set(jdbc.queryForObject("select pg_backend_pid()", Integer.class));
                locked.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                jdbc.update("update outbox_publish_cursor set last_published_sequence = 1 where stream = ? and aggregate_key = ?",
                        STREAM, market);
            }));

            try {
                assertTrue(locked.await(5, TimeUnit.SECONDS));
                assertEquals(Boolean.TRUE, jdbc.queryForObject("select pg_terminate_backend(?, 5000)", Boolean.class, backend.get()));
                assertTrue(coordinator.tryRun(STREAM, market,
                        cursor -> assertEquals(0L, cursor.lastPublishedSequence())));
            } finally {
                release.countDown();
            }

            assertThrows(ExecutionException.class, () -> oldOwner.get(5, TimeUnit.SECONDS));
        }

        assertEquals(0L, position(market));
    }

    private static String newMarket() {
        String market = UUID.randomUUID().toString();
        transaction.executeWithoutResult(status -> repository.initializeIfAbsent(STREAM, market));

        return market;
    }

    private static long position(String market) {
        return jdbc.queryForObject(
                "select last_published_sequence from outbox_publish_cursor where stream = ? and aggregate_key = ?",
                Long.class, STREAM, market
        );
    }
}
