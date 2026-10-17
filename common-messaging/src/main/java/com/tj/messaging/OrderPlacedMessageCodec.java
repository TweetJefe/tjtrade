package com.tj.messaging;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.common.enums.OrderType;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.cfg.EnumFeature;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public class OrderPlacedMessageCodec {

    private static final BigDecimal ORDER_VALUE_UPPER_BOUND = new BigDecimal("10000000000");
    private static final TypeReference<TradingMessages.Envelope<OrderPlacedEvent>> MESSAGE_TYPE =
            new TypeReference<>() {};

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(EnumFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    public String encode(TradingMessages.Envelope<OrderPlacedEvent> message) {
        validate(message);

        OrderPlacedEvent payload = message.payload();
        var normalizedPayload = new OrderPlacedEvent(
                payload.orderId(), payload.userId(), payload.symbol(), payload.side(), payload.type(),
                normalize(payload.price()), normalize(payload.quantity()), payload.timestamp()
        );
        var normalized = new TradingMessages.Envelope<>(
                message.eventId(), message.eventType(), message.schemaVersion(), message.stream(),
                message.aggregateKey(), message.sequence(), message.correlationId(), message.occurredAt(),
                normalizedPayload
        );

        return mapper.writeValueAsString(normalized);
    }

    public TradingMessages.Envelope<OrderPlacedEvent> decode(String json) {
        try {
            var message = mapper.readValue(json, MESSAGE_TYPE);
            validate(message);

            return message;
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Invalid order command envelope", exception);
        }
    }

    public String canonicalize(String json) {
        return encode(decode(json));
    }

    public String payloadHash(String json) {
        byte[] canonical = canonicalize(json).getBytes(StandardCharsets.UTF_8);

        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private BigDecimal normalize(BigDecimal value) {
        return new BigDecimal(value.stripTrailingZeros().toPlainString());
    }

    private void validate(TradingMessages.Envelope<OrderPlacedEvent> message) {
        if (message == null || message.eventId() == null || message.correlationId() == null
                || message.occurredAt() == null || message.sequence() <= 0
                || message.schemaVersion() != TradingMessages.SCHEMA_VERSION
                || !TradingMessages.MATCHING_COMMANDS.equals(message.stream())
                || !TradingMessages.ORDER_PLACED.equals(message.eventType())) {
            throw new IllegalArgumentException("Invalid order command metadata");
        }

        OrderPlacedEvent payload = message.payload();

        if (payload == null || payload.orderId() == null || payload.userId() == null
                || payload.side() == null || payload.type() != OrderType.LIMIT
                || payload.price() == null || payload.quantity() == null
                || payload.price().signum() <= 0 || payload.quantity().signum() <= 0
                || payload.price().compareTo(ORDER_VALUE_UPPER_BOUND) >= 0
                || payload.quantity().compareTo(ORDER_VALUE_UPPER_BOUND) >= 0
                || payload.price().stripTrailingZeros().scale() > 8
                || payload.quantity().stripTrailingZeros().scale() > 8
                || payload.symbol() == null || payload.symbol().isBlank()
                || !payload.symbol().equals(message.aggregateKey())
                || !payload.orderId().equals(message.correlationId())
                || !message.occurredAt().equals(payload.timestamp())) {
            throw new IllegalArgumentException("Invalid order command payload");
        }
    }
}
