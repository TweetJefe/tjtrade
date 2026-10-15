package com.tj.portfolioledger.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderStatus;
import com.tj.common.enums.OrderType;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.portfolioledger.messaging.outbox.OrderPlacedMessageCodec;
import com.tj.portfolioledger.messaging.outbox.OutboxWriter;
import com.tj.portfolioledger.repository.OutboxRepository;
import com.tj.portfolioledger.command.AcceptOrderCommand;
import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.enums.AcceptOrderOutcome;
import com.tj.portfolioledger.exception.AcceptOrderException;
import com.tj.portfolioledger.exception.ReserveFundsException;
import com.tj.portfolioledger.model.Order;
import com.tj.portfolioledger.repository.FundReservationRepository;
import com.tj.portfolioledger.repository.OrderRepository;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import com.tj.portfolioledger.result.AcceptOrderResult;
import com.tj.portfolioledger.service.AcceptOrderService;
import com.tj.portfolioledger.service.ReserveFundsService;
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
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AcceptOrderIT {

    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static AnnotationConfigApplicationContext context;
    private static DataSource dataSource;
    private static JdbcTemplate jdbc;
    private static AcceptOrderService service;

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
        context.getBeanFactory().setConversionService(DefaultConversionService.getSharedInstance());
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                "test-trading", Map.of("tjtrade.trading.symbols", "BTC_USDT,ETH_USDT")
        ));
        context.registerBean(DataSource.class, () -> dataSource);
        context.registerBean(DSLContext.class, () -> DSL.using(
                new TransactionAwareDataSourceProxy(dataSource), SQLDialect.POSTGRES
        ));
        context.register(TransactionConfiguration.class, OrderRepository.class,
                FundReservationRepository.class, WalletBalanceRepository.class,
                ReserveFundsService.class, OutboxRepository.class,
                OrderPlacedMessageCodec.class, OutboxWriter.class, AcceptOrderService.class);
        context.refresh();
        service = context.getBean(AcceptOrderService.class);
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
    void acceptsBuyOrderAndReservesQuoteAsset() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        var command = command(UUID.randomUUID(), userId, OrderSide.BUY, "100", "2");

        assertEquals(AcceptOrderOutcome.ACCEPTED, service.acceptOrder(command).outcome());

        Order order = readOrder(command.orderId());
        assertEquals(userId, order.getUserId());
        assertEquals("BTC_USDT", order.getSymbol());
        assertEquals(OrderSide.BUY, order.getSide());
        assertEquals(OrderType.LIMIT, order.getType());
        assertEquals(OrderStatus.NEW, order.getStatus());
        assertDecimal("100", order.getPrice());
        assertDecimal("2", order.getQuantity());
        assertDecimal("0", order.getFilledQuantity());
        assertNotNull(order.getCreatedAt());
        assertEquals(order.getCreatedAt(), order.getUpdatedAt());
        assertWallet(userId, "USDT", "800", "200");
        assertReservation(command.orderId(), userId, "USDT", "200");
    }

    @Test
    void acceptsSellOrderAndReservesBaseAsset() {
        UUID userId = createUser();
        createWallet(userId, "BTC", "10");
        createWallet(userId, "USDT", "1000");
        var command = command(UUID.randomUUID(), userId, OrderSide.SELL, "100", "2");

        assertEquals(AcceptOrderOutcome.ACCEPTED, service.acceptOrder(command).outcome());
        assertWallet(userId, "BTC", "8", "2");
        assertWallet(userId, "USDT", "1000", "0");
        assertReservation(command.orderId(), userId, "BTC", "2");
    }

    @Test
    void retriesIgnoreDecimalScaleAndDoNotResetOrderLifecycle() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2"));
        Instant createdAt = readOrder(orderId).getCreatedAt();
        var originalOutbox = outboxRow(orderId);
        long originalSequence = marketSequence("BTC_USDT");

        assertEquals(AcceptOrderOutcome.ALREADY_ACCEPTED,
                service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100.00", "2.000")).outcome());
        assertWallet(userId, "USDT", "800", "200");

        jdbc.update("update orders set status = 'FILLED', filled_quantity = quantity where id = ?", orderId);
        jdbc.update("update fund_reservation set status = 'CONSUMED', remaining_amount = 0 where order_id = ?", orderId);
        jdbc.update("update wallet_balances set locked = 0 where user_id = ? and currency = 'USDT'", userId);

        assertEquals(AcceptOrderOutcome.ALREADY_ACCEPTED,
                service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")).outcome());
        assertEquals(OrderStatus.FILLED, readOrder(orderId).getStatus());
        assertDecimal("2", readOrder(orderId).getFilledQuantity());
        assertEquals(createdAt, readOrder(orderId).getCreatedAt());
        var retriedOutbox = outboxRow(orderId);
        assertEquals(originalOutbox, retriedOutbox);
        assertEquals(originalSequence, marketSequence("BTC_USDT"));
        assertOutboxCount(orderId, 1);
        assertWallet(userId, "USDT", "800", "0");
        assertCounts(userId, 1, 1);
    }

    @Test
    void rejectsChangedOrderParametersEvenWhenReservedAmountMatches() {
        UUID userId = createUser();
        UUID otherUserId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2"));

        for (var changed : List.of(
                command(orderId, userId, OrderSide.BUY, "50", "4"),
                command(orderId, userId, OrderSide.SELL, "100", "2"),
                command(orderId, otherUserId, OrderSide.BUY, "100", "2"),
                new AcceptOrderCommand(orderId, userId, "ETH_USDT", OrderSide.BUY,
                        OrderType.LIMIT, new BigDecimal("100"), new BigDecimal("2")))) {
            var failure = assertThrows(AcceptOrderException.class, () -> service.acceptOrder(changed));
            assertEquals(AcceptOrderException.Reason.ORDER_CONFLICT, failure.getReason());
        }

        assertWallet(userId, "USDT", "800", "200");
        assertCounts(userId, 1, 1);
        assertCounts(otherUserId, 0, 0);
    }

    @Test
    void insufficientFundsRollBackOrderAndReservationAndAllowRetry() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "100");
        UUID orderId = UUID.randomUUID();

        var failure = assertThrows(ReserveFundsException.class,
                () -> service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")));
        assertEquals(ReserveFundsException.Reason.INSUFFICIENT_FUNDS_OR_WALLET_MISSING, failure.getReason());
        assertCounts(userId, 0, 0);
        assertWallet(userId, "USDT", "100", "0");

        assertEquals(AcceptOrderOutcome.ACCEPTED,
                service.acceptOrder(command(orderId, userId, OrderSide.BUY, "10", "2")).outcome());
        assertWallet(userId, "USDT", "80", "20");
    }

    @Test
    void missingWalletRollsBackOrderAndDoesNotCreateMoney() {
        UUID userId = createUser();

        assertThrows(ReserveFundsException.class,
                () -> service.acceptOrder(command(UUID.randomUUID(), userId, OrderSide.BUY, "100", "2")));
        assertCounts(userId, 0, 0);
        assertEquals(0, jdbc.queryForObject("select count(*) from wallet_balances where user_id = ?", Integer.class, userId));
    }

    @Test
    void rejectsInvalidUnsupportedAndUnrepresentableOrders() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        var invalid = new ArrayList<AcceptOrderCommand>();
        invalid.add(null);
        invalid.add(command(null, userId, OrderSide.BUY, "1", "1"));
        invalid.add(command(UUID.randomUUID(), null, OrderSide.BUY, "1", "1"));
        invalid.add(command(UUID.randomUUID(), userId, null, "1", "1"));
        invalid.add(command(UUID.randomUUID(), userId, OrderSide.BUY, null, "1"));
        invalid.add(command(UUID.randomUUID(), userId, OrderSide.BUY, "1", null));

        for (String value : List.of("0", "-1", "0.000000001", "10000000000")) {
            invalid.add(command(UUID.randomUUID(), userId, OrderSide.BUY, value, "1"));
            invalid.add(command(UUID.randomUUID(), userId, OrderSide.BUY, "1", value));
        }

        invalid.add(command(UUID.randomUUID(), userId, OrderSide.BUY, "0.00000001", "0.1"));

        for (String symbol : List.of("", "BTC", "BTC-USDT", "BTC_USDT_EXTRA", "btc_usdt", "BTC_BTC", "A".repeat(20) + "_B")) {
            invalid.add(new AcceptOrderCommand(UUID.randomUUID(), userId, symbol,
                    OrderSide.BUY, OrderType.LIMIT, BigDecimal.ONE, BigDecimal.ONE));
        }

        invalid.add(new AcceptOrderCommand(UUID.randomUUID(), userId, "BTC_USDT",
                OrderSide.BUY, null, BigDecimal.ONE, BigDecimal.ONE));

        for (var command : invalid) {
            assertThrows(IllegalArgumentException.class, () -> service.acceptOrder(command));
        }

        var market = new AcceptOrderCommand(UUID.randomUUID(), userId, "BTC_USDT",
                OrderSide.BUY, OrderType.MARKET, BigDecimal.ONE, BigDecimal.ONE);
        assertEquals(AcceptOrderException.Reason.UNSUPPORTED_ORDER_TYPE,
                assertThrows(AcceptOrderException.class, () -> service.acceptOrder(market)).getReason());

        var unsupported = new AcceptOrderCommand(UUID.randomUUID(), userId, "LTC_USDT",
                OrderSide.BUY, OrderType.LIMIT, BigDecimal.ONE, BigDecimal.ONE);
        assertEquals(AcceptOrderException.Reason.UNSUPPORTED_SYMBOL,
                assertThrows(AcceptOrderException.class, () -> service.acceptOrder(unsupported)).getReason());
        assertCounts(userId, 0, 0);
        assertWallet(userId, "USDT", "1000", "0");
    }

    @Test
    void reusesMatchingPreexistingReservationWithoutLockingFundsAgain() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        context.getBean(ReserveFundsService.class)
                .reserveFunds(new ReserveFundsCommand(orderId, userId, "USDT", new BigDecimal("200")));

        assertEquals(AcceptOrderOutcome.ACCEPTED,
                service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")).outcome());
        assertWallet(userId, "USDT", "800", "200");
        assertCounts(userId, 1, 1);
    }

    @Test
    void conflictingPreexistingReservationRollsBackNewOrder() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        context.getBean(ReserveFundsService.class)
                .reserveFunds(new ReserveFundsCommand(orderId, userId, "USDT", new BigDecimal("99")));

        assertEquals(ReserveFundsException.Reason.CONFLICT,
                assertThrows(ReserveFundsException.class,
                        () -> service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2"))).getReason());
        assertCounts(userId, 0, 1);
        assertWallet(userId, "USDT", "901", "99");
    }

    @Test
    void existingOrderWithoutReservationIsNotReportedAsAccepted() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        context.getBean(OrderRepository.class).insertIfAbsent(Order.builder()
                .id(orderId)
                .userId(userId)
                .symbol("BTC_USDT")
                .side(OrderSide.BUY)
                .type(OrderType.LIMIT)
                .price(new BigDecimal("100"))
                .quantity(new BigDecimal("2"))
                .build());

        assertEquals(AcceptOrderException.Reason.INCONSISTENT_RESERVATION,
                assertThrows(AcceptOrderException.class,
                        () -> service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2"))).getReason());
        assertCounts(userId, 1, 0);
        assertWallet(userId, "USDT", "1000", "0");
    }

    @Test
    void concurrentDuplicatesCreateOneOrderAndOneReservation() throws Exception {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        var futures = concurrentAcceptances(userId, List.of(
                command(orderId, userId, OrderSide.BUY, "100", "2"),
                command(orderId, userId, OrderSide.BUY, "100.00", "2.000")));
        var outcomes = new ArrayList<AcceptOrderOutcome>();

        for (var future : futures) {
            outcomes.add(future.get(20, TimeUnit.SECONDS).outcome());
        }

        assertEquals(1, outcomes.stream().filter(outcome -> outcome == AcceptOrderOutcome.ACCEPTED).count());
        assertEquals(1, outcomes.stream().filter(outcome -> outcome == AcceptOrderOutcome.ALREADY_ACCEPTED).count());
        assertOutboxCount(orderId, 1);
        assertCounts(userId, 1, 1);
        assertWallet(userId, "USDT", "800", "200");
    }

    @Test
    void concurrentOrdersCannotOverspendAndRejectedOrderIsRolledBack() throws Exception {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        var futures = concurrentAcceptances(userId, List.of(
                command(UUID.randomUUID(), userId, OrderSide.BUY, "300", "2"),
                command(UUID.randomUUID(), userId, OrderSide.BUY, "300", "2")));
        int accepted = 0;
        int rejected = 0;

        for (var future : futures) {
            try {
                assertEquals(AcceptOrderOutcome.ACCEPTED, future.get(20, TimeUnit.SECONDS).outcome());
                accepted++;
            } catch (ExecutionException exception) {
                var failure = assertInstanceOf(ReserveFundsException.class, exception.getCause());
                assertEquals(ReserveFundsException.Reason.INSUFFICIENT_FUNDS_OR_WALLET_MISSING, failure.getReason());
                rejected++;
            }
        }

        assertEquals(1, accepted);
        assertEquals(1, rejected);
        assertCounts(userId, 1, 1);
        assertWallet(userId, "USDT", "400", "600");
    }


    @Test
    void outboxEnvelopeMatchesPersistedOrderAndCanonicalHash() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100.00", "2.000"));
        var stored = outboxRow(orderId);
        String json = stored.get("payload").toString();
        var codec = context.getBean(OrderPlacedMessageCodec.class);
        var message = codec.decode(json);
        Order order = readOrder(orderId);

        assertEquals(stored.get("event_id"), message.eventId());
        assertNotEquals(orderId, message.eventId());
        assertEquals(TradingMessages.MATCHING_COMMANDS, message.stream());
        assertEquals(TradingMessages.ORDER_PLACED, message.eventType());
        assertEquals(TradingMessages.SCHEMA_VERSION, message.schemaVersion());
        assertEquals("BTC_USDT", message.aggregateKey());
        assertEquals(orderId, message.correlationId());
        assertEquals(stored.get("sequence"), message.sequence());
        assertEquals(order.getCreatedAt(), message.occurredAt());
        assertEquals(orderId, message.payload().orderId());
        assertEquals(userId, message.payload().userId());
        assertEquals(order.getSide(), message.payload().side());
        assertEquals(order.getType(), message.payload().type());
        assertDecimal("100", message.payload().price());
        assertDecimal("2", message.payload().quantity());
        assertNull(stored.get("published_at"));
        assertEquals(stored.get("payload_hash"), codec.payloadHash(json));
        assertEquals(codec.encode(message), codec.canonicalize(json));
        assertTrue(codec.canonicalize(json).contains("\"price\":100,"));

        var differentScale = new TradingMessages.Envelope<>(
                message.eventId(), message.eventType(), message.schemaVersion(), message.stream(),
                message.aggregateKey(), message.sequence(), message.correlationId(), message.occurredAt(),
                new OrderPlacedEvent(orderId, userId, "BTC_USDT", OrderSide.BUY, OrderType.LIMIT,
                        new BigDecimal("100.00000000"), new BigDecimal("2.00"), order.getCreatedAt())
        );
        assertEquals(codec.canonicalize(json), codec.encode(differentScale));
        assertEquals(codec.payloadHash(json), codec.payloadHash(codec.encode(differentScale)));
    }

    @Test
    void marketSequencesAreIndependent() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        long btcBefore = marketSequence("BTC_USDT");
        long ethBefore = marketSequence("ETH_USDT");
        UUID btcOrderId = UUID.randomUUID();
        UUID ethOrderId = UUID.randomUUID();
        service.acceptOrder(command(btcOrderId, userId, OrderSide.BUY, "10", "1"));
        service.acceptOrder(new AcceptOrderCommand(ethOrderId, userId, "ETH_USDT", OrderSide.BUY,
                OrderType.LIMIT, BigDecimal.TEN, BigDecimal.ONE));

        assertEquals(btcBefore + 1, outboxRow(btcOrderId).get("sequence"));
        assertEquals(ethBefore + 1, outboxRow(ethOrderId).get("sequence"));
        assertEquals(btcBefore + 1, marketSequence("BTC_USDT"));
        assertEquals(ethBefore + 1, marketSequence("ETH_USDT"));
    }

    @Test
    void outboxInsertionFailureRollsBackFundsOrderReservationAndSequence() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        long sequenceBefore = marketSequence("BTC_USDT");
        String constraint = "test_outbox_rejection_" + orderId.toString().replace("-", "");
        jdbc.execute("alter table outbox_event add constraint " + constraint
                + " check (correlation_id <> '" + orderId + "'::uuid)");

        try {
            assertThrows(org.jooq.exception.DataAccessException.class,
                    () -> service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")));
        } finally {
            jdbc.execute("alter table outbox_event drop constraint " + constraint);
        }

        assertCounts(userId, 0, 0);
        assertWallet(userId, "USDT", "1000", "0");
        assertOutboxCount(orderId, 0);
        assertEquals(sequenceBefore, marketSequence("BTC_USDT"));
        assertEquals(AcceptOrderOutcome.ACCEPTED,
                service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")).outcome());
        assertEquals(sequenceBefore + 1, outboxRow(orderId).get("sequence"));
    }

    @Test
    void outerRollbackDiscardsSuccessfulAcceptanceAndItsOutbox() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        long sequenceBefore = marketSequence("BTC_USDT");
        var transaction = new org.springframework.transaction.support.TransactionTemplate(
                context.getBean(PlatformTransactionManager.class)
        );
        transaction.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.execute(status -> {
            assertEquals(AcceptOrderOutcome.ACCEPTED,
                    service.acceptOrder(command(orderId, userId, OrderSide.BUY, "100", "2")).outcome());
            status.setRollbackOnly();

            return null;
        });

        assertCounts(userId, 0, 0);
        assertWallet(userId, "USDT", "1000", "0");
        assertOutboxCount(orderId, 0);
        assertEquals(sequenceBefore, marketSequence("BTC_USDT"));
    }

    @Test
    void outboxWriterRequiresAnExistingTransaction() {
        assertThrows(org.springframework.transaction.IllegalTransactionStateException.class,
                () -> context.getBean(OutboxWriter.class).appendOrderPlaced(Order.builder().build()));
    }

    @Test
    void acceptedOrderWithoutOutboxHistoryIsRejectedInsteadOfRepublished() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        var command = command(orderId, userId, OrderSide.BUY, "100", "2");
        service.acceptOrder(command);
        long sequence = marketSequence("BTC_USDT");
        jdbc.update("delete from outbox_event where correlation_id = ?", orderId);

        assertEquals(AcceptOrderException.Reason.INCONSISTENT_OUTBOX,
                assertThrows(AcceptOrderException.class, () -> service.acceptOrder(command)).getReason());
        assertWallet(userId, "USDT", "800", "200");
        assertOutboxCount(orderId, 0);
        assertEquals(sequence, marketSequence("BTC_USDT"));
    }

    @Test
    void corruptedOutboxPayloadOrMetadataIsDetectedOnRetry() {
        UUID userId = createUser();
        createWallet(userId, "USDT", "1000");
        UUID orderId = UUID.randomUUID();
        var command = command(orderId, userId, OrderSide.BUY, "100", "2");
        service.acceptOrder(command);
        var original = outboxRow(orderId);

        for (String mutation : List.of(
                "payload = jsonb_set(payload, '{payload,quantity}', '3'::jsonb)",
                "payload_hash = '" + "0".repeat(64) + "'",
                "aggregate_key = 'ETH_USDT'",
                "schema_version = 2")) {
            jdbc.update("update outbox_event set " + mutation + " where correlation_id = ?", orderId);

            try {
                assertEquals(AcceptOrderException.Reason.INCONSISTENT_OUTBOX,
                        assertThrows(AcceptOrderException.class, () -> service.acceptOrder(command)).getReason());
            } finally {
                jdbc.update("update outbox_event set payload = ?::jsonb, payload_hash = ?, aggregate_key = ?, schema_version = ? where correlation_id = ?",
                        original.get("payload").toString(), original.get("payload_hash"),
                        original.get("aggregate_key"), original.get("schema_version"), orderId);
            }
        }

        assertWallet(userId, "USDT", "800", "200");
        assertOutboxCount(orderId, 1);
        assertEquals(AcceptOrderOutcome.ALREADY_ACCEPTED, service.acceptOrder(command).outcome());
    }

    @Test
    void concurrentAcceptedOrdersGetContiguousMarketSequences() throws Exception {
        UUID seedUserId = createUser();
        createWallet(seedUserId, "USDT", "10");
        service.acceptOrder(command(UUID.randomUUID(), seedUserId, OrderSide.BUY, "1", "1"));

        UUID firstUserId = createUser();
        UUID secondUserId = createUser();
        createWallet(firstUserId, "USDT", "1000");
        createWallet(secondUserId, "USDT", "1000");
        UUID firstOrderId = UUID.randomUUID();
        UUID secondOrderId = UUID.randomUUID();
        long before = marketSequence("BTC_USDT");
        var futures = concurrentAcceptances(
                "select last_sequence from outbox_sequence where stream = 'matching.commands' and aggregate_key = ? for update",
                "BTC_USDT", List.of(
                        command(firstOrderId, firstUserId, OrderSide.BUY, "100", "2"),
                        command(secondOrderId, secondUserId, OrderSide.BUY, "100", "2"))
        );

        for (var future : futures) {
            assertEquals(AcceptOrderOutcome.ACCEPTED, future.get(20, TimeUnit.SECONDS).outcome());
        }

        var sequences = jdbc.queryForList(
                "select sequence from outbox_event where correlation_id in (?, ?) order by sequence",
                Long.class, firstOrderId, secondOrderId
        );
        assertEquals(List.of(before + 1, before + 2), sequences);
        assertWallet(firstUserId, "USDT", "800", "200");
        assertWallet(secondUserId, "USDT", "800", "200");
        assertCounts(firstUserId, 1, 1);
        assertCounts(secondUserId, 1, 1);
    }


    private static Map<String, Object> outboxRow(UUID orderId) {
        return jdbc.queryForMap("select * from outbox_event where correlation_id = ?", orderId);
    }

    private static void assertOutboxCount(UUID orderId, int expected) {
        assertEquals(expected, jdbc.queryForObject(
                "select count(*) from outbox_event where correlation_id = ?", Integer.class, orderId
        ));
    }

    private static long marketSequence(String symbol) {
        var sequences = jdbc.queryForList(
                "select last_sequence from outbox_sequence where stream = ? and aggregate_key = ?",
                Long.class, TradingMessages.MATCHING_COMMANDS, symbol
        );

        return sequences.isEmpty() ? 0 : sequences.getFirst();
    }

    private static AcceptOrderCommand command(UUID orderId, UUID userId, OrderSide side, String price, String quantity) {
        return new AcceptOrderCommand(orderId, userId, "BTC_USDT", side, OrderType.LIMIT,
                price == null ? null : new BigDecimal(price), quantity == null ? null : new BigDecimal(quantity));
    }

    private static UUID createUser() {
        UUID userId = UUID.randomUUID();
        jdbc.update("insert into users (id, email, password_hash) values (?, ?, ?)",
                userId, userId + "@accept-order.test", "test-fixture-password-hash");

        return userId;
    }

    private static void createWallet(UUID userId, String asset, String available) {
        jdbc.update("insert into wallet_balances (user_id, currency, available) values (?, ?, ?)",
                userId, asset, new BigDecimal(available));
    }

    private static Order readOrder(UUID orderId) {
        var transaction = new org.springframework.transaction.support.TransactionTemplate(
                context.getBean(PlatformTransactionManager.class)
        );

        return transaction.execute(status -> context.getBean(OrderRepository.class)
                .findByOrderIdForUpdate(orderId)
                .orElseThrow());
    }

    private static void assertWallet(UUID userId, String asset, String available, String locked) {
        var wallet = jdbc.queryForMap("select available, locked from wallet_balances where user_id = ? and currency = ?",
                userId, asset);
        assertDecimal(available, (BigDecimal) wallet.get("available"));
        assertDecimal(locked, (BigDecimal) wallet.get("locked"));
    }

    private static void assertReservation(UUID orderId, UUID userId, String asset, String amount) {
        var reservation = jdbc.queryForMap("select * from fund_reservation where order_id = ?", orderId);
        assertEquals(userId, reservation.get("user_id"));
        assertEquals(asset, reservation.get("asset"));
        assertEquals("ACTIVE", reservation.get("status"));
        assertDecimal(amount, (BigDecimal) reservation.get("amount"));
        assertDecimal(amount, (BigDecimal) reservation.get("remaining_amount"));
    }

    private static void assertCounts(UUID userId, int orders, int reservations) {
        assertEquals(orders, jdbc.queryForObject("select count(*) from orders where user_id = ?", Integer.class, userId));
        assertEquals(reservations, jdbc.queryForObject("select count(*) from fund_reservation where user_id = ?", Integer.class, userId));
    }

    private static void assertDecimal(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }

    private static List<Future<AcceptOrderResult>> concurrentAcceptances(
            UUID userId, List<AcceptOrderCommand> commands
    ) throws Exception {
        return concurrentAcceptances(
                "select user_id from wallet_balances where user_id = ? and currency = 'USDT' for update",
                userId, commands
        );
    }

    private static List<Future<AcceptOrderResult>> concurrentAcceptances(
            String lockSql, Object lockValue, List<AcceptOrderCommand> commands
    ) throws Exception {
        try (var connection = dataSource.getConnection(); var executor = Executors.newFixedThreadPool(commands.size())) {
            connection.setAutoCommit(false);

            try (var lock = connection.prepareStatement(lockSql)) {
                lock.setObject(1, lockValue);

                try (var result = lock.executeQuery()) {
                    assertTrue(result.next());
                }
            }

            var ready = new CountDownLatch(commands.size());
            var start = new CountDownLatch(1);
            var futures = commands.stream().map(command -> executor.submit(() -> {
                ready.countDown();

                if (!start.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Concurrent start timed out");
                }

                return service.acceptOrder(command);
            })).toList();

            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                Instant limit = Instant.now().plusSeconds(8);
                boolean overlapping = false;

                while (Instant.now().isBefore(limit)) {
                    Integer waiting = jdbc.queryForObject("select count(*) from pg_stat_activity where wait_event_type = 'Lock'", Integer.class);

                    if (waiting != null && waiting >= commands.size()) {
                        overlapping = true;
                        break;
                    }

                    Thread.sleep(25);
                }

                assertTrue(overlapping, "Both acceptance transactions must overlap on database locks");
            } finally {
                start.countDown();
                connection.rollback();
            }

            return futures;
        }
    }
}
