package com.tj.integration;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderStatus;
import com.tj.common.enums.OrderType;
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
                ReserveFundsService.class, AcceptOrderService.class);
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
        try (var connection = dataSource.getConnection(); var executor = Executors.newFixedThreadPool(commands.size())) {
            connection.setAutoCommit(false);

            try (var lock = connection.prepareStatement("select user_id from wallet_balances where user_id = ? and currency = 'USDT' for update")) {
                lock.setObject(1, userId);

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
