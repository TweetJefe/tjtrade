package com.tj.integration;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.common.grpc.UserServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.DirectoryResourceAccessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PlatformIT {
    private static final Path ROOT = Path.of(System.getProperty("project.root"));
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(System.getProperty("postgres.image"));
    private static final GenericContainer<?> REDIS = new GenericContainer<>(System.getProperty("redis.image")).withExposedPorts(6379);
    private static final KafkaContainer KAFKA = new KafkaContainer(System.getProperty("kafka.image"));
    private static final Map<String, Process> APPS = new HashMap<>();
    private static final Map<String, Integer> PORTS = new HashMap<>();
    private static int userGrpc;
    private static int ledgerGrpc;

    @BeforeAll
    static void startPlatform() throws Exception {
        String configuredHost = System.getenv("DOCKER_HOST");
        if (configuredHost != null && !configuredHost.isBlank()) {
            assertEquals(URI.create(configuredHost), DockerClientFactory.instance().getTransportConfig().getDockerHost(),
                    "Configured DOCKER_HOST is unavailable; refusing to use another Docker engine");
        }
        DockerClientFactory.instance().client().pingCmd().exec();
        try {
            POSTGRES.start();
            REDIS.start();
            KAFKA.start();
            migrate("user-service");
            migrate("portfolio-ledger-service");
            userGrpc = freePort();
            ledgerGrpc = freePort();
            for (String module : List.of("user-service", "portfolio-ledger-service", "order-risk-service",
                    "matching-engine-service", "market-data-service", "api-gateway")) {
                startApp(module);
            }
        } catch (Exception | AssertionError failure) {
            stopPlatform();
            throw failure;
        }
    }

    private static void migrate(String module) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var resources = new DirectoryResourceAccessor(ROOT.resolve(module).resolve("src/main/resources"))) {
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try (var migration = new Liquibase("db/changelog/db.changelog-master.xml", resources, database)) {
                migration.update(new Contexts(), new LabelExpression());
                migration.validate();
            }
        }
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }

    private static void startApp(String module) throws Exception {
        int port = freePort();
        PORTS.put(module, port);
        Path log = ROOT.resolve("integration-tests/target/startup-logs/" + module + ".log");
        Files.createDirectories(log.getParent());
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-jar", ROOT.resolve(module + "/target/" + module + "-" + System.getProperty("project.version") + "-exec.jar").toString(),
                "--server.port=" + port, "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(), "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.data.redis.host=" + REDIS.getHost(), "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.auto-offset-reset=earliest", "--spring.liquibase.enabled=false"));
        if (module.equals("user-service") || module.equals("portfolio-ledger-service")) {
            command.add("--spring.grpc.server.port=" + (module.equals("user-service") ? userGrpc : ledgerGrpc));
            command.add("--spring.profiles.active=tls");
        }
        if (module.equals("order-risk-service")) {
            command.add("--spring.profiles.active=tls");
            command.add("--spring.grpc.client.channel.user-service.target=static://localhost:" + userGrpc);
            command.add("--spring.grpc.client.channel.portfolio-ledger-service.target=static://localhost:" + ledgerGrpc);
        }
        var builder = new ProcessBuilder(command).directory(ROOT.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("JWTSECRET", java.util.Base64.getEncoder().encodeToString(
                "platform-smoke-test-secret-at-least-32-bytes-long".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        builder.environment().put("GRPC_TLS_CERT", ROOT.resolve("integration-tests/src/test/resources/tls/server.crt").toString());
        builder.environment().put("GRPC_TLS_KEY", ROOT.resolve("integration-tests/src/test/resources/tls/server.key").toString());
        builder.environment().put("GRPC_TLS_CA", ROOT.resolve("integration-tests/src/test/resources/tls/server.crt").toString());
        Process process = builder.start();
        APPS.put(module, process);
        Instant limit = Instant.now().plusSeconds(90);
        while (Instant.now().isBefore(limit)) {
            if (!process.isAlive()) { fail(module + " exited. Startup log:\n" + Files.readString(log)); }
            if (Files.readString(log).contains("Started ")) { return; }
            Thread.sleep(250);
        }
        fail(module + " did not start. Startup log:\n" + Files.readString(log));
    }

    @AfterAll
    static void stopPlatform() {
        APPS.values().forEach(process -> {
            process.destroy();
            try { if (!process.waitFor(10, TimeUnit.SECONDS)) { process.destroyForcibly().waitFor(); } }
            catch (InterruptedException exception) { Thread.currentThread().interrupt(); process.destroyForcibly(); }
        });
        APPS.clear();
        KAFKA.stop();
        REDIS.stop();
        POSTGRES.stop();
    }

    @Test
    void allSixPackagedApplicationsServeHttp() throws Exception {
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        for (var app : APPS.entrySet()) {
            assertTrue(app.getValue().isAlive(), app.getKey());
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + PORTS.get(app.getKey()) + "/__platform_probe__"))
                    .timeout(Duration.ofSeconds(10)).build();
            assertEquals(404, client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode(), app.getKey());
        }
    }

    private ManagedChannel tlsChannel(int port) throws Exception {
        return NettyChannelBuilder.forAddress("localhost", port)
                .sslContext(GrpcSslContexts.forClient().trustManager(ROOT.resolve("integration-tests/src/test/resources/tls/server.crt").toFile()).build()).build();
    }

    @Test
    void generatedGrpcStubsCallBothServicesOverTls() throws Exception {
        ManagedChannel user = tlsChannel(userGrpc);
        ManagedChannel ledger = tlsChannel(ledgerGrpc);
        try {
            assertFalse(UserServiceGrpc.newBlockingStub(user).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .checkUserExists(CheckUserRequest.newBuilder().setUserId(UUID.randomUUID().toString()).build()).getExists());
            assertTrue(PortfolioServiceGrpc.newBlockingStub(ledger).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .lockFunds(LockFundsRequest.newBuilder().setUserId(UUID.randomUUID().toString()).setAsset("USDT")
                            .setAmount("1.00").setOrderId(UUID.randomUUID().toString()).build()).getSuccess());
        } finally { user.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); ledger.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
    }

    @Test
    void migrationsAreRepeatableAndCreatePostgresTypes() throws Exception {
        migrate("user-service");
        migrate("portfolio-ledger-service");
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement()) {
            try (var result = statement.executeQuery("select count(*) from databasechangelog")) { assertTrue(result.next()); assertEquals(6, result.getInt(1)); }
            try (var result = statement.executeQuery("select data_type from information_schema.columns where table_name='users' and column_name='id'")) { assertTrue(result.next()); assertEquals("uuid", result.getString(1)); }
            try (var result = statement.executeQuery("select data_type from information_schema.columns where table_name='users' and column_name='created_at'")) { assertTrue(result.next()); assertEquals("timestamp with time zone", result.getString(1)); }
        }
    }

    @Test
    void generatedSchemaMatchesMigratedPostgres() throws Exception {
        Map<String, org.jooq.Table<?>> tables = new HashMap<>();
        for (Class<?> contracts : List.of(com.tj.user.jooq.Tables.class, com.tj.portfolioledger.jooq.Tables.class)) {
            for (var reference : contracts.getFields()) {
                var table = (org.jooq.Table<?>) reference.get(null);
                tables.put(table.getName(), table);
            }
        }
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var actualTables = new java.util.HashSet<String>();
            try (var metadata = connection.getMetaData().getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (metadata.next()) {
                    String name = metadata.getString("TABLE_NAME");
                    if (!name.startsWith("databasechangelog")) { actualTables.add(name); }
                }
            }
            assertEquals(tables.keySet(), actualTables, "Codegen inputs must cover the migration history");
            for (var table : tables.values()) {
                var columns = new java.util.HashSet<String>();
                try (var metadata = connection.getMetaData().getColumns(null, "public", table.getName(), "%")) {
                    while (metadata.next()) {
                        String name = metadata.getString("COLUMN_NAME");
                        columns.add(name);
                        var field = table.field(name);
                        assertNotNull(field, table.getName() + "." + name);
                        Class<?> expected = switch (metadata.getString("TYPE_NAME")) {
                            case "uuid" -> UUID.class;
                            case "timestamptz" -> java.time.OffsetDateTime.class;
                            case "numeric" -> BigDecimal.class;
                            case "varchar" -> String.class;
                            default -> throw new AssertionError("Add an explicit type assertion for " + name);
                        };
                        assertEquals(expected, field.getType(), table.getName() + "." + name);
                        assertEquals(metadata.getInt("NULLABLE") == java.sql.DatabaseMetaData.columnNullable,
                                field.getDataType().nullable(), table.getName() + "." + name);
                        if (expected == BigDecimal.class) {
                            assertEquals(metadata.getInt("COLUMN_SIZE"), field.getDataType().precision());
                            assertEquals(metadata.getInt("DECIMAL_DIGITS"), field.getDataType().scale());
                        }
                    }
                }
                assertEquals(java.util.Arrays.stream(table.fields()).map(org.jooq.Field::getName)
                        .collect(java.util.stream.Collectors.toSet()), columns);
            }
        }
    }

    @Test
    void tlsRejectsUntrustedCertificate() throws Exception {
        var channel = NettyChannelBuilder.forAddress("localhost", userGrpc).useTransportSecurity().build();
        try {
            var failure = assertThrows(io.grpc.StatusRuntimeException.class, () -> UserServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS).checkUserExists(CheckUserRequest.newBuilder()
                            .setUserId(UUID.randomUUID().toString()).build()));
            assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
        } finally { channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
    }

    @Test
    void jacksonThreeKafkaRoundTripPreservesDecimalUuidAndInstant() throws Exception {
        String topic = "platform-round-trip-" + UUID.randomUUID();
        var event = new OrderPlacedEvent(UUID.randomUUID(), UUID.randomUUID(), "BTC-USDT", OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("12345.12345678"), new BigDecimal("0.12345678"), Instant.parse("2026-01-01T00:00:00.123456Z"));
        Map<String, Object> producerProperties = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        Map<String, Object> consumerProperties = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "platform-" + UUID.randomUUID(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var deserializer = new JacksonJsonDeserializer<>(OrderPlacedEvent.class);
        deserializer.addTrustedPackages("com.tj.common.event");
        try (var producer = new KafkaProducer<String, OrderPlacedEvent>(producerProperties, new StringSerializer(), new JacksonJsonSerializer<>());
             var consumer = new KafkaConsumer<String, OrderPlacedEvent>(consumerProperties, new StringDeserializer(), deserializer)) {
            producer.send(new ProducerRecord<>(topic, event.orderId().toString(), event)).get(15, TimeUnit.SECONDS);
            consumer.subscribe(List.of(topic));
            Instant limit = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(limit)) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) { assertEquals(event, records.iterator().next().value()); return; }
            }
            fail("Kafka did not return the serialized event");
        }
    }
}
