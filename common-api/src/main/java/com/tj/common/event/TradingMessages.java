package com.tj.common.event;

import java.time.Instant;
import java.util.UUID;

public final class TradingMessages {

    public static final String MATCHING_COMMANDS = "matching.commands";
    public static final String ORDER_PLACED = "OrderPlaced";
    public static final int SCHEMA_VERSION = 1;

    private TradingMessages() {
    }

    public record Envelope<T>(
            UUID eventId,
            String eventType,
            int schemaVersion,
            String stream,
            String aggregateKey,
            long sequence,
            UUID correlationId,
            Instant occurredAt,
            T payload
    ) {
    }
}
