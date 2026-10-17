package com.tj.portfolioledger.messaging.outbox;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.common.event.TradingMessages;
import com.tj.portfolioledger.model.Order;
import com.tj.portfolioledger.repository.OutboxRepository;
import com.tj.portfolioledger.repository.OutboxPublishCursorRepository;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OutboxPublisherIT {

    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static final KafkaContainer KAFKA = new KafkaContainer(System.getProperty("kafka.image"))
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    private static AnnotationConfigApplicationContext context;
    private static DataSource dataSource;
    private static JdbcTemplate jdbc;
    private static TransactionTemplate transaction;
    private static OutboxPublisher publisher;

    @BeforeAll
    static void startDatabaseAndServices() throws Exception {
        String configuredHost = System.getenv("DOCKER_HOST");

        if (configuredHost != null && !configuredHost.isBlank()) {
            assertEquals(URI.create(configuredHost), DockerClientFactory.instance().getTransportConfig().getDockerHost());
        }

        DockerClientFactory.instance().client().pingCmd().exec();
        POSTGRES.start();
        KAFKA.start();

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TradingMessages.MATCHING_COMMANDS, 3, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
        }

        var warmup = producerProperties();
        warmup.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "warmup-" + UUID.randomUUID());
        warmup.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 15000L);

        try (var producer = new KafkaProducer<String, String>(warmup, new StringSerializer(), new StringSerializer())) {
            producer.initTransactions();
        }

        var source = new PGSimpleDataSource();
        source.setUrl(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        source.setOptions("-c statement_timeout=15000 -c lock_timeout=12000");
        dataSource = source;
        jdbc = new JdbcTemplate(dataSource);

        for (String module : List.of("user-service", "portfolio-ledger-service")) {
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
        context.registerBean(DSLContext.class, () -> DSL.using(
                new TransactionAwareDataSourceProxy(dataSource), SQLDialect.POSTGRES
        ));
        context.registerBean(FencedCommandProducerFactory.class,
                () -> new FencedCommandProducerFactory(producerProperties(), "publisher-it"));
        context.register(TransactionConfiguration.class, OutboxRepository.class, OutboxPublishCursorRepository.class,
                OutboxPublicationCoordinator.class, OrderPlacedMessageCodec.class, OutboxWriter.class, OutboxPublisher.class);
        context.refresh();
        publisher = context.getBean(OutboxPublisher.class);
        transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @AfterAll
    static void stopDatabaseAndServices() {
        if (context != null) {
            context.close();
        }

        KAFKA.stop();
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
    void publishesCanonicalCommandsInMarketOrderAndSavesReceipts() throws Exception {
        String symbol = symbol();
        UUID first = append(symbol);
        UUID second = append(symbol);

        assertTrue(publisher.publishNext(symbol));
        assertTrue(publisher.publishNext(symbol));
        assertFalse(publisher.publishNext(symbol));
        var messages = values(symbol, 2);
        var codec = context.getBean(OrderPlacedMessageCodec.class);
        assertEquals(List.of(1L, 2L), messages.stream().map(value -> codec.decode(value).sequence()).toList());
        assertEquals(first, codec.decode(messages.get(0)).correlationId());
        assertEquals(second, codec.decode(messages.get(1)).correlationId());
        assertEquals(2L, position(symbol));
        assertEquals(2, jdbc.queryForObject("select count(*) from outbox_event where aggregate_key = ? and published_at is not null",
                Integer.class, symbol));
    }

    @Test
    void kafkaFailureLeavesCommandPendingForRetry() throws Exception {
        String symbol = symbol();
        append(symbol);
        var failing = publisher(new FencedCommandProducerFactory(Map.of("bootstrap.servers", "127.0.0.1:1"), "publisher-it"));

        assertThrows(Exception.class, () -> failing.publishNext(symbol));
        assertEquals(0L, position(symbol));
        assertEquals(0, jdbc.queryForObject("select count(*) from outbox_event where aggregate_key = ? and published_at is not null",
                Integer.class, symbol));
        assertTrue(publisher.publishNext(symbol));
        assertEquals(1, values(symbol, 1).size());
    }

    @Test
    void brokerCommitBeforeSqlFailureCausesStableDuplicateRatherThanLostCommand() throws Exception {
        String symbol = symbol();
        UUID orderId = append(symbol);
        jdbc.execute("alter table outbox_event add constraint forced_receipt_failure check (correlation_id <> '"
                + orderId + "' or published_at is null)");

        try {
            assertThrows(Exception.class, () -> publisher.publishNext(symbol));
            assertEquals(0L, position(symbol));
            assertEquals(1, values(symbol, 1).size());
        } finally {
            jdbc.execute("alter table outbox_event drop constraint forced_receipt_failure");
        }

        assertTrue(publisher.publishNext(symbol));
        var messages = values(symbol, 2);
        assertEquals(2, messages.size());
        assertEquals(messages.getFirst(), messages.getLast());
        assertEquals(1L, position(symbol));
    }

    @Test
    void corruptHashAndSequenceGapCannotBeSkipped() {
        String corrupted = symbol();
        append(corrupted);
        jdbc.update("update outbox_event set payload_hash = ? where aggregate_key = ?", "0".repeat(64), corrupted);
        assertThrows(Exception.class, () -> publisher.publishNext(corrupted));
        assertEquals(0L, position(corrupted));

        String gap = symbol();
        UUID first = append(gap);
        append(gap);
        jdbc.update("delete from outbox_event where correlation_id = ?", first);
        assertThrows(Exception.class, () -> publisher.publishNext(gap));
        assertEquals(0L, position(gap));
    }

    @Test
    void competingPublishersDoNotPublishTheNextCommandTwice() throws Exception {
        String symbol = symbol();
        append(symbol);
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> { start.await(); return publisher.publishNext(symbol); });
            var second = executor.submit(() -> { start.await(); return publisher.publishNext(symbol); });
            start.countDown();
            assertNotEquals(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        }

        assertEquals(1L, position(symbol));
        assertEquals(1, values(symbol, 1).size());
    }

    @Test
    void stableTransactionalIdFencesOldProducerAndHidesAbortedData() throws Exception {
        String symbol = symbol();
        var factory = context.getBean(FencedCommandProducerFactory.class);
        var old = factory.open(TradingMessages.MATCHING_COMMANDS, symbol);
        var replacement = factory.open(TradingMessages.MATCHING_COMMANDS, symbol);

        try {
            old.initTransactions();
            old.beginTransaction();
            old.send(new ProducerRecord<>(TradingMessages.MATCHING_COMMANDS, symbol, "old-uncommitted")).get(2, TimeUnit.SECONDS);
            replacement.initTransactions();
            assertThrows(KafkaException.class, old::commitTransaction);
            replacement.beginTransaction();
            replacement.send(new ProducerRecord<>(TradingMessages.MATCHING_COMMANDS, symbol, "new-committed")).get(2, TimeUnit.SECONDS);
            replacement.commitTransaction();

            assertEquals(List.of("new-committed"), values(symbol, 1));
        } finally {
            old.close(Duration.ofSeconds(1));
            replacement.close(Duration.ofSeconds(1));
        }
    }

    @Test
    void sessionLossDuringFencingCannotSendUsingStaleCursor() throws Exception {
        String symbol = symbol();
        append(symbol);
        var paused = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var backend = new AtomicInteger();
        var delayedFactory = new FencedCommandProducerFactory(producerProperties(), "publisher-it") {
            @Override
            public KafkaProducer<String, String> open(String stream, String aggregateKey) {
                var properties = producerProperties();
                properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(stream, aggregateKey));

                return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer()) {
                    @Override
                    public void initTransactions() {
                        super.initTransactions();
                        backend.set(jdbc.queryForObject("select pg_backend_pid()", Integer.class));
                        paused.countDown();

                        try {
                            if (!release.await(8, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Delayed producer was not released");
                            }
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(exception);
                        }
                    }
                };
            }
        };

        try (var executor = Executors.newSingleThreadExecutor()) {
            var old = executor.submit(() -> publisher(delayedFactory).publishNext(symbol));

            try {
                assertTrue(paused.await(8, TimeUnit.SECONDS));
                assertEquals(Boolean.TRUE, jdbc.queryForObject("select pg_terminate_backend(?, 5000)", Boolean.class, backend.get()));
                assertTrue(publisher.publishNext(symbol));
            } finally {
                release.countDown();
            }

            assertThrows(ExecutionException.class, () -> old.get(10, TimeUnit.SECONDS));
        }

        assertEquals(1L, position(symbol));
        assertEquals(1, values(symbol, 1).size());
    }

    private static OutboxPublisher publisher(FencedCommandProducerFactory factory) {
        return new OutboxPublisher(context.getBean(OutboxRepository.class), context.getBean(OutboxPublishCursorRepository.class),
                context.getBean(OutboxPublicationCoordinator.class), context.getBean(OrderPlacedMessageCodec.class), factory);
    }

    private static Map<String, Object> producerProperties() {
        return new HashMap<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private static UUID append(String symbol) {
        UUID orderId = UUID.randomUUID();
        var order = Order.builder().id(orderId).userId(UUID.randomUUID()).symbol(symbol).side(OrderSide.BUY)
                .type(OrderType.LIMIT).price(new BigDecimal("100")).quantity(BigDecimal.ONE)
                .createdAt(Instant.parse("2026-10-17T10:00:00Z")).build();
        transaction.executeWithoutResult(status -> context.getBean(OutboxWriter.class).appendOrderPlaced(order));

        return orderId;
    }

    private static long position(String symbol) {
        return jdbc.queryForObject("select last_published_sequence from outbox_publish_cursor where stream = ? and aggregate_key = ?",
                Long.class, TradingMessages.MATCHING_COMMANDS, symbol);
    }

    private static List<String> values(String symbol, int expected) {
        Map<String, Object> properties = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false, ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        var result = new ArrayList<String>();

        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
            consumer.assign(List.of(new TopicPartition(TradingMessages.MATCHING_COMMANDS, 0),
                    new TopicPartition(TradingMessages.MATCHING_COMMANDS, 1), new TopicPartition(TradingMessages.MATCHING_COMMANDS, 2)));
            consumer.seekToBeginning(consumer.assignment());
            Instant limit = Instant.now().plusSeconds(12);

            while (Instant.now().isBefore(limit) && result.size() < expected) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (symbol.equals(record.key())) {
                        result.add(record.value());
                    }
                }
            }
        }

        assertEquals(expected, result.size());

        return result;
    }

    private static String symbol() {
        return "T" + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase() + "_USDT";
    }
}
