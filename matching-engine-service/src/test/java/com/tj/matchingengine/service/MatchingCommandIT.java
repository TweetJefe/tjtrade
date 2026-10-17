package com.tj.matchingengine.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.matchingengine.repository.MatchingRepository;
import com.tj.messaging.OrderPlacedMessageCodec;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MatchingCommandIT {

    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static final OrderPlacedMessageCodec CODEC = new OrderPlacedMessageCodec();
    private static AnnotationConfigApplicationContext context;
    private static DataSource dataSource;
    private static JdbcTemplate jdbc;
    private static MatchingCommandService service;

    @BeforeAll
    static void startDatabaseAndServices() throws Exception {
        String configuredHost = System.getenv("DOCKER_HOST");

        if (configuredHost != null && !configuredHost.isBlank()) {
            assertEquals(URI.create(configuredHost), DockerClientFactory.instance().getTransportConfig().getDockerHost());
        }

        DockerClientFactory.instance().client().pingCmd().exec();
        POSTGRES.start();

        var source = new PGSimpleDataSource();
        source.setUrl(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        source.setOptions("-c statement_timeout=15000 -c lock_timeout=12000");
        dataSource = source;
        jdbc = new JdbcTemplate(dataSource);

        for (String module : List.of("matching-engine-service")) {
            try (var connection = dataSource.getConnection();
                 var resources = new DirectoryResourceAccessor(ROOT.resolve(module).resolve("src/main/resources"))) {
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

                try (var migration = new Liquibase("db/changelog/db.changelog-master.xml", resources, database)) {
                    migration.update(new Contexts(), new LabelExpression());
                }
            }
        }

        context = new AnnotationConfigApplicationContext();
        context.registerBean(DataSource.class, () -> dataSource);
        context.register(TransactionConfiguration.class, MatchingRepository.class, MatchingCommandService.class);
        context.registerBean(JdbcTemplate.class, () -> jdbc);
        context.refresh();
        service = context.getBean(MatchingCommandService.class);
    }

    @AfterAll
    static void stopDatabaseAndServices() {
        if (context != null) {
            context.close();
        }

        POSTGRES.stop();
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
    void duplicateDeliveryDoesNotRepeatMatchingAndSurvivesNewServiceInstance() {
        String symbol = symbol();
        var command = command(symbol, 1, OrderSide.BUY, "100", "2");

        assertTrue(service.apply(symbol, CODEC.encode(command)));
        assertFalse(service.apply(symbol, CODEC.encode(command)));
        assertEquals(1, count("matching_order", symbol));
        assertEquals(1, count("matching_inbox", symbol));
        assertEquals(1, count("matching_result_outbox", symbol));
        var restored = new MatchingEngineServiceImpl(context.getBean(MatchingRepository.class));
        assertEquals(0, new BigDecimal("2").compareTo(restored.getOrderBook(symbol).bids().getFirst().quantity()));
    }

    @Test
    void matchesByPriceThenAdmissionSequenceAndPersistsBothOrderIds() {
        String symbol = symbol();
        var first = command(symbol, 1, OrderSide.SELL, "100", "1");
        var second = command(symbol, 2, OrderSide.SELL, "100", "2");
        var expensive = command(symbol, 3, OrderSide.SELL, "101", "1");
        service.apply(symbol, CODEC.encode(first));
        service.apply(symbol, CODEC.encode(second));
        service.apply(symbol, CODEC.encode(expensive));
        var buy = command(symbol, 4, OrderSide.BUY, "100", "2");

        service.apply(symbol, CODEC.encode(buy));

        assertEquals(0, BigDecimal.ZERO.compareTo(remaining(first.correlationId())));
        assertEquals(0, BigDecimal.ONE.compareTo(remaining(second.correlationId())));
        assertEquals(0, BigDecimal.ONE.compareTo(remaining(expensive.correlationId())));
        assertEquals(0, BigDecimal.ZERO.compareTo(remaining(buy.correlationId())));
        assertEquals(2, jdbc.queryForObject("select jsonb_array_length(payload->'fills') from matching_result_outbox where command_event_id = ?",
                Integer.class, buy.eventId()));
        assertEquals(buy.correlationId().toString(), jdbc.queryForObject(
                "select payload->'fills'->0->>'buyOrderId' from matching_result_outbox where command_event_id = ?", String.class, buy.eventId()));
        assertEquals(first.correlationId().toString(), jdbc.queryForObject(
                "select payload->'fills'->0->>'sellOrderId' from matching_result_outbox where command_event_id = ?", String.class, buy.eventId()));
    }

    @Test
    void changedDuplicateAndSequenceGapFailWithoutEffects() {
        String symbol = symbol();
        var first = command(symbol, 1, OrderSide.BUY, "100", "2");
        service.apply(symbol, CODEC.encode(first));
        var payload = first.payload();
        var changed = new TradingMessages.Envelope<>(first.eventId(), first.eventType(), first.schemaVersion(),
                first.stream(), symbol, 1L, first.correlationId(), first.occurredAt(), new OrderPlacedEvent(
                payload.orderId(), payload.userId(), symbol, payload.side(), payload.type(), new BigDecimal("101"),
                payload.quantity(), payload.timestamp()));

        assertThrows(IllegalStateException.class, () -> service.apply(symbol, CODEC.encode(changed)));
        assertThrows(IllegalStateException.class, () -> service.apply(symbol, CODEC.encode(command(symbol, 3, OrderSide.SELL, "100", "1"))));
        assertEquals(1, count("matching_inbox", symbol));
        assertEquals(1L, jdbc.queryForObject("select last_sequence from matching_market where symbol = ?", Long.class, symbol));
    }

    @Test
    void invalidEnvelopeAndWrongKeyNeverMutateBook() {
        String symbol = symbol();
        String json = CODEC.encode(command(symbol, 1, OrderSide.BUY, "100", "1"));

        assertThrows(IllegalArgumentException.class, () -> service.apply(symbol, "{}"));
        assertThrows(IllegalArgumentException.class, () -> service.apply("OTHER_USDT", json));
        assertEquals(0, count("matching_order", symbol));
    }

    @Test
    void resultPersistenceFailureRollsBackFillsInboxAndCursor() {
        String symbol = symbol();
        var sell = command(symbol, 1, OrderSide.SELL, "100", "2");
        service.apply(symbol, CODEC.encode(sell));
        var buy = command(symbol, 2, OrderSide.BUY, "100", "1");
        jdbc.execute("alter table matching_result_outbox add constraint forced_result_failure check (command_event_id <> '" + buy.eventId() + "')");

        try {
            assertThrows(RuntimeException.class, () -> service.apply(symbol, CODEC.encode(buy)));
            assertEquals(0, new BigDecimal("2").compareTo(remaining(sell.correlationId())));
            assertEquals(1, count("matching_order", symbol));
            assertEquals(1, count("matching_inbox", symbol));
            assertEquals(1L, jdbc.queryForObject("select last_sequence from matching_market where symbol = ?", Long.class, symbol));
        } finally {
            jdbc.execute("alter table matching_result_outbox drop constraint forced_result_failure");
        }

        assertTrue(service.apply(symbol, CODEC.encode(buy)));
        assertFalse(service.apply(symbol, CODEC.encode(buy)));
    }

    @Test
    void concurrentDuplicateCommandsApplyExactlyOneDatabaseEffect() throws Exception {
        String symbol = symbol();
        var command = command(symbol, 1, OrderSide.BUY, "100", "1");
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> { start.await(); return service.apply(symbol, CODEC.encode(command)); });
            var second = executor.submit(() -> { start.await(); return service.apply(symbol, CODEC.encode(command)); });
            start.countDown();
            assertNotEquals(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        }

        assertEquals(1, count("matching_inbox", symbol));
        assertEquals(1, count("matching_order", symbol));
    }

    @Test
    void duplicateWithMissingResultFailsClosed() {
        String symbol = symbol();
        var message = command(symbol, 1, OrderSide.BUY, "100", "1");
        service.apply(symbol, CODEC.encode(message));
        jdbc.update("delete from matching_result_outbox where command_event_id = ?", message.eventId());

        assertThrows(IllegalStateException.class, () -> service.apply(symbol, CODEC.encode(message)));
        assertEquals(1, count("matching_inbox", symbol));
    }

    @Test
    void listenerAcknowledgesOnlyAfterSqlCommitAndNeverOnFailure() {
        String symbol = symbol();
        var message = command(symbol, 1, OrderSide.BUY, "100", "1");
        var listener = new com.tj.matchingengine.kafka.MatchingEngineKafkaListener(service);
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<>(TradingMessages.MATCHING_COMMANDS,
                0, 0L, symbol, CODEC.encode(message));
        var acknowledged = new java.util.concurrent.atomic.AtomicBoolean();

        listener.handle(record, () -> {
            assertFalse(org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive());
            assertEquals(1, count("matching_inbox", symbol));
            acknowledged.set(true);
        });
        assertTrue(acknowledged.get());
        acknowledged.set(false);
        var invalid = new org.apache.kafka.clients.consumer.ConsumerRecord<>(TradingMessages.MATCHING_COMMANDS,
                0, 1L, symbol, "{}");

        assertThrows(IllegalArgumentException.class, () -> listener.handle(invalid, () -> acknowledged.set(true)));
        assertFalse(acknowledged.get());
    }

    private static TradingMessages.Envelope<OrderPlacedEvent> command(String symbol, long sequence, OrderSide side, String price, String quantity) {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.parse("2026-10-17T10:00:00Z");

        return new TradingMessages.Envelope<>(UUID.randomUUID(), TradingMessages.ORDER_PLACED, 1,
                TradingMessages.MATCHING_COMMANDS, symbol, sequence, orderId, now,
                new OrderPlacedEvent(orderId, UUID.randomUUID(), symbol, side, OrderType.LIMIT,
                        new BigDecimal(price), new BigDecimal(quantity), now));
    }

    private static String symbol() {
        return "T" + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase() + "_USDT";
    }

    private static int count(String table, String symbol) {
        return jdbc.queryForObject("select count(*) from " + table + " where symbol = ?", Integer.class, symbol);
    }

    private static BigDecimal remaining(UUID orderId) {
        return jdbc.queryForObject("select remaining_quantity from matching_order where order_id = ?", BigDecimal.class, orderId);
    }
}
