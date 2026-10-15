package com.tj.portfolioledger.service;

import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.LockFundsResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.portfolioledger.grpc.PortfolioGrpcServiceImpl;
import com.tj.portfolioledger.repository.FundReservationRepository;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
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
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ReserveFundsIT {

    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static AnnotationConfigApplicationContext context;
    private static DataSource dataSource;
    private static Server grpcServer;

    @BeforeAll
    static void startDatabaseAndGrpcService() throws Exception {
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
            dataSource = source;

            for (String module : List.of("user-service", "portfolio-ledger-service")) {
                try (var connection = databaseConnection();
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
            context.register(TransactionConfiguration.class, FundReservationRepository.class,
                    WalletBalanceRepository.class, ReserveFundsService.class);
            context.refresh();

            grpcServer = NettyServerBuilder.forPort(0)
                    .addService(new PortfolioGrpcServiceImpl(context.getBean(ReserveFundsService.class)))
                    .build()
                    .start();
        } catch (Exception | AssertionError failure) {
            stopDatabaseAndGrpcService();
            throw failure;
        }
    }

    @AfterAll
    static void stopDatabaseAndGrpcService() throws InterruptedException {
        try {
            if (grpcServer != null) {
                grpcServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally {
            try {
                if (context != null) {
                    context.close();
                }
            } finally {
                POSTGRES.stop();
            }
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
    void reservesFundsAndDeduplicatesDifferentDecimalScales() throws Exception {
        UUID userId = createFundedWallet("1000");
        UUID orderId = UUID.randomUUID();
        withPortfolioClient(client -> {
            assertTrue(client.lockFunds(reservationRequest(userId, orderId, "USDT", "100")).getSuccess());
            assertWallet(userId, "900", "100");
            assertActiveReservation(orderId, userId, "100");
            var updatedAt = walletUpdatedAt(userId);
            assertTrue(updatedAt.isAfter(Instant.parse("2000-01-01T00:00:00Z")));

            assertTrue(client.lockFunds(reservationRequest(userId, orderId, "USDT", "100.00")).getSuccess());
            assertWallet(userId, "900", "100");
            assertActiveReservation(orderId, userId, "100");
            assertEquals(updatedAt, walletUpdatedAt(userId), "A duplicate must not update the wallet");
            assertEquals(1, reservationCount(userId));
        });
    }

    @Test
    void rejectsConflictingReservationParameters() throws Exception {
        UUID userId = createFundedWallet("1000");
        UUID otherUserId = createFundedWallet("1000");
        UUID orderId = UUID.randomUUID();
        withPortfolioClient(client -> {
            assertTrue(client.lockFunds(reservationRequest(userId, orderId, "USDT", "100")).getSuccess());
            for (var request : List.of(
                    reservationRequest(userId, orderId, "USDT", "101"),
                    reservationRequest(userId, orderId, "BTC", "100"),
                    reservationRequest(otherUserId, orderId, "USDT", "100"))) {
                var failure = assertThrows(StatusRuntimeException.class, () -> client.lockFunds(request));
                assertEquals(Status.Code.ALREADY_EXISTS, failure.getStatus().getCode());
            }
            assertWallet(userId, "900", "100");
            assertWallet(otherUserId, "1000", "0");
            assertActiveReservation(orderId, userId, "100");
            assertEquals(1, reservationCount(userId));
            assertEquals(0, reservationCount(otherUserId));
        });
    }

    @Test
    void insufficientFundsRollBackReservationAndAllowRetry() throws Exception {
        UUID userId = createFundedWallet("1000");
        UUID orderId = UUID.randomUUID();
        withPortfolioClient(client -> {
            assertFalse(client.lockFunds(reservationRequest(userId, orderId, "USDT", "2000")).getSuccess());
            assertWallet(userId, "1000", "0");
            assertEquals(Instant.parse("2000-01-01T00:00:00Z"), walletUpdatedAt(userId));
            assertEquals(0, reservationCount(userId));

            // The failed transaction must not permanently occupy this order ID.
            assertTrue(client.lockFunds(reservationRequest(userId, orderId, "USDT", "100")).getSuccess());
            assertWallet(userId, "900", "100");
            assertActiveReservation(orderId, userId, "100");
        });
    }

    @Test
    void missingWalletDoesNotCreateBalanceOrReservation() throws Exception {
        UUID userId = createUser();
        withPortfolioClient(client -> assertFalse(client.lockFunds(
                reservationRequest(userId, UUID.randomUUID(), "USDT", "1")).getSuccess()));
        assertEquals(0, reservationCount(userId));
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("select count(*) from wallet_balances where user_id = ?")) {
            statement.setObject(1, userId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(0, result.getInt(1));
            }
        }
    }

    @Test
    void rejectsInvalidReservationRequestsWithoutChangingMoney() throws Exception {
        UUID userId = createFundedWallet("1000");
        var requests = new ArrayList<LockFundsRequest>();
        for (String amount : List.of("0", "-1", "0.000000001", "100000000000000000000", "not-a-number")) {
            requests.add(reservationRequest(userId, UUID.randomUUID(), "USDT", amount));
        }
        for (String asset : List.of("", " USDT", "USDT ", "A".repeat(21))) {
            requests.add(reservationRequest(userId, UUID.randomUUID(), asset, "1"));
        }
        requests.add(reservationRequest(userId, UUID.randomUUID(), "USDT", "1")
                .toBuilder().setOrderId("not-a-uuid").build());
        requests.add(reservationRequest(userId, UUID.randomUUID(), "USDT", "1")
                .toBuilder().setUserId("not-a-uuid").build());

        withPortfolioClient(client -> {
            for (var request : requests) {
                var failure = assertThrows(StatusRuntimeException.class, () -> client.lockFunds(request));
                assertEquals(Status.Code.INVALID_ARGUMENT, failure.getStatus().getCode());
            }
        });
        assertWallet(userId, "1000", "0");
        assertEquals(0, reservationCount(userId));
    }

    @Test
    void concurrentDuplicateRequestsReserveFundsOnce() throws Exception {
        UUID userId = createFundedWallet("1000");
        UUID orderId = UUID.randomUUID();
        withPortfolioClient(client -> {
            var responses = concurrentReservations(client, userId, List.of(
                    reservationRequest(userId, orderId, "USDT", "100"),
                    reservationRequest(userId, orderId, "USDT", "100.00")));
            assertTrue(responses.stream().allMatch(LockFundsResponse::getSuccess));
        });
        assertWallet(userId, "900", "100");
        assertActiveReservation(orderId, userId, "100");
        assertEquals(1, reservationCount(userId));
    }

    @Test
    void concurrentDifferentOrdersCannotOverspend() throws Exception {
        UUID userId = createFundedWallet("150");
        withPortfolioClient(client -> {
            var responses = concurrentReservations(client, userId, List.of(
                    reservationRequest(userId, UUID.randomUUID(), "USDT", "100"),
                    reservationRequest(userId, UUID.randomUUID(), "USDT", "100")));
            assertEquals(1, responses.stream().filter(LockFundsResponse::getSuccess).count());
        });
        assertWallet(userId, "50", "100");
        assertEquals(1, reservationCount(userId));
    }

    @Test
    void databaseFailureRollsBackReservationAndReturnsInternalStatus() throws Exception {
        UUID userId = createFundedWallet("1000");
        String maximumLocked = "99999999999999999999.99999999";
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("update wallet_balances set locked = ? where user_id = ?")) {
            statement.setBigDecimal(1, new BigDecimal(maximumLocked));
            statement.setObject(2, userId);
            assertEquals(1, statement.executeUpdate());
        }
        withPortfolioClient(client -> {
            var failure = assertThrows(StatusRuntimeException.class, () -> client.lockFunds(
                    reservationRequest(userId, UUID.randomUUID(), "USDT", "1")));
            assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
            assertEquals("Failed to reserve funds", failure.getStatus().getDescription());
        });
        assertWallet(userId, "1000", maximumLocked);
        assertEquals(0, reservationCount(userId));
    }

    private static Connection databaseConnection() throws Exception {
        return dataSource.getConnection();
    }

    private static UUID createUser() throws Exception {
        UUID userId = UUID.randomUUID();
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("insert into users (id, email, password_hash) values (?, ?, ?)")) {
            statement.setObject(1, userId);
            statement.setString(2, userId + "@reservation.test");
            statement.setString(3, "test-fixture-password-hash");
            assertEquals(1, statement.executeUpdate());
        }
        return userId;
    }

    private static UUID createFundedWallet(String available) throws Exception {
        UUID userId = createUser();
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("""
                     insert into wallet_balances (user_id, currency, available, locked, created_at, updated_at)
                     values (?, 'USDT', ?, 0, timestamptz '2000-01-01 00:00:00+00', timestamptz '2000-01-01 00:00:00+00')
                     """)) {
            statement.setObject(1, userId);
            statement.setBigDecimal(2, new BigDecimal(available));
            assertEquals(1, statement.executeUpdate());
        }
        return userId;
    }

    private static LockFundsRequest reservationRequest(UUID userId, UUID orderId, String asset, String amount) {
        return LockFundsRequest.newBuilder().setUserId(userId.toString()).setOrderId(orderId.toString())
                .setAsset(asset).setAmount(amount).build();
    }

    private void withPortfolioClient(ReservationScenario scenario) throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .build();
        try {
            scenario.run(PortfolioServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface
    private interface ReservationScenario {
        void run(PortfolioServiceGrpc.PortfolioServiceBlockingStub client) throws Exception;
    }

    private static void assertWallet(UUID userId, String available, String locked) throws Exception {
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("select available, locked from wallet_balances where user_id = ? and currency = 'USDT'")) {
            statement.setObject(1, userId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(0, new BigDecimal(available).compareTo(result.getBigDecimal("available")));
                assertEquals(0, new BigDecimal(locked).compareTo(result.getBigDecimal("locked")));
                assertFalse(result.next());
            }
        }
    }

    private static Instant walletUpdatedAt(UUID userId) throws Exception {
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("select updated_at from wallet_balances where user_id = ? and currency = 'USDT'")) {
            statement.setObject(1, userId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getObject(1, java.time.OffsetDateTime.class).toInstant();
            }
        }
    }

    private static void assertActiveReservation(UUID orderId, UUID userId, String amount) throws Exception {
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("select * from fund_reservation where order_id = ?")) {
            statement.setObject(1, orderId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(userId, result.getObject("user_id", UUID.class));
                assertEquals("USDT", result.getString("asset"));
                assertEquals(0, new BigDecimal(amount).compareTo(result.getBigDecimal("amount")));
                assertEquals(0, new BigDecimal(amount).compareTo(result.getBigDecimal("remaining_amount")));
                assertEquals("ACTIVE", result.getString("status"));
                assertFalse(result.next());
            }
        }
    }

    private static int reservationCount(UUID userId) throws Exception {
        try (var connection = databaseConnection();
             var statement = connection.prepareStatement("select count(*) from fund_reservation where user_id = ?")) {
            statement.setObject(1, userId);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getInt(1);
            }
        }
    }

    private static List<LockFundsResponse> concurrentReservations(
            PortfolioServiceGrpc.PortfolioServiceBlockingStub client, UUID userId,
            List<LockFundsRequest> requests) throws Exception {
        try (var connection = databaseConnection(); var executor = Executors.newFixedThreadPool(requests.size())) {
            connection.setAutoCommit(false);
            try (var lock = connection.prepareStatement("select user_id from wallet_balances where user_id = ? and currency = 'USDT' for update")) {
                lock.setObject(1, userId);
                try (var result = lock.executeQuery()) { assertTrue(result.next()); }
            }

            var ready = new CountDownLatch(requests.size());
            var start = new CountDownLatch(1);
            var futures = requests.stream().map(request -> executor.submit(() -> {
                ready.countDown();
                if (!start.await(5, TimeUnit.SECONDS)) { throw new IllegalStateException("Concurrent start timed out"); }
                return client.lockFunds(request);
            })).toList();

            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();
                awaitReservationLockWaiters(requests.size());
            } finally {
                start.countDown();
                connection.rollback();
            }

            var responses = new ArrayList<LockFundsResponse>();
            for (var future : futures) { responses.add(future.get(20, TimeUnit.SECONDS)); }
            return responses;
        }
    }

    private static void awaitReservationLockWaiters(int expected) throws Exception {
        Instant limit = Instant.now().plusSeconds(10);
        try (var connection = databaseConnection(); var statement = connection.createStatement()) {
            while (Instant.now().isBefore(limit)) {
                try (var result = statement.executeQuery("""
                        select count(*) from pg_stat_activity
                        where wait_event_type = 'Lock'
                          and (query like '%wallet_balances%' or query like '%fund_reservation%')
                        """)) {
                    assertTrue(result.next());
                    if (result.getInt(1) >= expected) { return; }
                }
                Thread.sleep(25);
            }
        }
        fail("Reservation requests did not overlap while waiting on database locks");
    }
}
