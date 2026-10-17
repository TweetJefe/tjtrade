package com.tj.portfolioledger.messaging.outbox;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

public class FencedCommandProducerFactory {

    private final Map<String, Object> configuration;
    private final String namespace;

    public FencedCommandProducerFactory(Map<String, Object> configuration, String namespace) {
        if (namespace == null || !namespace.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Kafka deployment namespace is invalid");
        }

        this.configuration = Map.copyOf(configuration);
        this.namespace = namespace;
    }

    public KafkaProducer<String, String> open(String stream, String aggregateKey) {
        var properties = new HashMap<>(configuration);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(stream, aggregateKey));
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000L);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 6000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
    }

    public String transactionalId(String stream, String aggregateKey) {
        try {
            byte[] identity = (namespace + "\u0000" + stream + "\u0000" + aggregateKey).getBytes(StandardCharsets.UTF_8);
            String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(identity));

            return "tjtrade-command-" + namespace + "-" + digest;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
