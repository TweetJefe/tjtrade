package com.tj.portfolioledger.service;

import com.tj.portfolioledger.exception.WalletException;
import com.tj.portfolioledger.grpc.WalletUserClient;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WalletServiceIT {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static AnnotationConfigApplicationContext context;
    private static JdbcTemplate jdbc;
    private static WalletService service;
    private static WalletUserClient identity;

    @BeforeAll
    static void start() throws Exception {
        String host = System.getenv("DOCKER_HOST");

        if (host != null && !host.isBlank()) {
            assertEquals(URI.create(host), DockerClientFactory.instance().getTransportConfig().getDockerHost());
        }

        DockerClientFactory.instance().client().pingCmd().exec();
        POSTGRES.start();
        var source = new PGSimpleDataSource();
        source.setUrl(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        source.setOptions("-c statement_timeout=15000 -c lock_timeout=12000");
        jdbc = new JdbcTemplate(source);
        Path root = Path.of(System.getProperty("project.root"));

        for (String module : List.of("user-service", "portfolio-ledger-service")) {
            try (var connection = source.getConnection();
                 var resources = new DirectoryResourceAccessor(root.resolve(module).resolve("src/main/resources"))) {
                var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

                try (var migration = new Liquibase("db/changelog/db.changelog-master.xml", resources, database)) {
                    migration.update(new Contexts(), new LabelExpression());
                }
            }
        }

        identity = mock(WalletUserClient.class);
        context = new AnnotationConfigApplicationContext();
        context.registerBean(DataSource.class, () -> source);
        context.registerBean(DSLContext.class, () -> DSL.using(new TransactionAwareDataSourceProxy(source), SQLDialect.POSTGRES));
        context.registerBean(WalletUserClient.class, () -> identity);
        context.register(Transactions.class, WalletBalanceRepository.class, WalletCreationService.class, WalletService.class);
        context.refresh();
        service = context.getBean(WalletService.class);
    }

    @AfterAll
    static void stop() {
        if (context != null) {
            context.close();
        }

        POSTGRES.stop();
    }

    @BeforeEach
    void identityCallsDoNotHoldSqlTransaction() {
        reset(identity);
        doAnswer(invocation -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            return null;
        }).when(identity).requireActive(any());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class Transactions {
        @Bean
        PlatformTransactionManager transactionManager(DataSource source) {
            return new DataSourceTransactionManager(source);
        }
    }

    @Test
    void createsZeroWalletAndRetryPreservesIdentityBalancesAndTimestamp() {
        UUID userId = user();
        var first = service.create(userId, "USDT");
        assertTrue(first.created());
        assertEquals(new BigDecimal("0.00000000"), first.wallet().total());
        jdbc.update("update wallet_balances set available = 20, locked = 3 where id = ?", first.wallet().walletId());
        var before = jdbc.queryForMap("select * from wallet_balances where id = ?", first.wallet().walletId());
        var repeat = service.create(userId, "USDT");

        assertFalse(repeat.created());
        assertEquals(first.wallet().walletId(), repeat.wallet().walletId());
        assertEquals(new BigDecimal("20.00000000"), repeat.wallet().available());
        assertEquals(new BigDecimal("3.00000000"), repeat.wallet().locked());
        assertEquals(before, jdbc.queryForMap("select * from wallet_balances where id = ?", first.wallet().walletId()));
    }

    @Test
    void concurrentCreationHasOneWinnerAndOneStableWallet() throws Exception {
        UUID userId = user();
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(8)) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<com.tj.portfolioledger.result.WalletResults.Creation>>();

            for (int i = 0; i < 8; i++) {
                tasks.add(executor.submit(() -> { start.await(); return service.create(userId, "BTC"); }));
            }

            start.countDown();
            int created = 0;
            var identities = new java.util.HashSet<UUID>();

            for (var task : tasks) {
                var result = task.get(10, TimeUnit.SECONDS);
                created += result.created() ? 1 : 0;
                identities.add(result.wallet().walletId());
            }

            assertEquals(1, created);
            assertEquals(1, identities.size());
        }

        assertEquals(1, count(userId));
    }

    @Test
    void listAndLookupAreRestrictedToOwnerAndSorted() {
        UUID owner = user();
        UUID other = user();
        assertTrue(service.list(owner).isEmpty());
        var wallet = service.create(owner, "USDT").wallet();
        service.create(owner, "BTC");
        service.create(other, "ETH");

        assertEquals(List.of("BTC", "USDT"), service.list(owner).stream().map(value -> value.currency()).toList());
        assertEquals(wallet, service.get(owner, wallet.walletId()));
        assertEquals(WalletException.Reason.WALLET_NOT_FOUND,
                assertThrows(WalletException.class, () -> service.get(other, wallet.walletId())).getReason());
        assertThrows(WalletException.class, () -> service.get(owner, UUID.randomUUID()));
    }

    @Test
    void invalidCurrencyIsRejectedBeforeIdentityOrSqlMutation() {
        UUID userId = user();
        assertThrows(IllegalArgumentException.class, () -> service.create(userId, "usd"));
        verifyNoInteractions(identity);
        assertEquals(0, count(userId));
    }

    @Test
    void inactiveMissingOrUnavailableIdentityCannotCreateWallet() {
        UUID userId = user();

        for (var reason : List.of(WalletException.Reason.USER_NOT_ACTIVE, WalletException.Reason.USER_NOT_FOUND, WalletException.Reason.IDENTITY_UNAVAILABLE)) {
            doThrow(new WalletException(reason)).when(identity).requireActive(userId);
            assertEquals(reason, assertThrows(WalletException.class, () -> service.create(userId, "ETH")).getReason());
            assertEquals(0, count(userId));
        }
    }

    @Test
    void userDeletedAfterIdentityCheckCannotCreateOrphanWallet() {
        UUID userId = user();
        doAnswer(invocation -> {
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            jdbc.update("delete from users where id = ?", userId);
            return null;
        }).when(identity).requireActive(userId);

        assertThrows(org.jooq.exception.IntegrityConstraintViolationException.class, () -> service.create(userId, "USDT"));
        assertEquals(0, count(userId));
    }

    private UUID user() {
        UUID userId = UUID.randomUUID();
        jdbc.update("insert into users (id, email, password_hash) values (?, ?, 'fixture')", userId, userId + "@wallet.test");

        return userId;
    }

    private int count(UUID userId) {
        return jdbc.queryForObject("select count(*) from wallet_balances where user_id = ?", Integer.class, userId);
    }
}
