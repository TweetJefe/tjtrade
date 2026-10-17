package com.tj.matchingengine.model;

import com.tj.common.enums.OrderSide;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class MatchingState {

    private MatchingState() {
    }

    public record Order(
            UUID orderId,
            UUID userId,
            String symbol,
            OrderSide side,
            BigDecimal price,
            BigDecimal remainingQuantity,
            long acceptedSequence,
            Instant createdAt
    ) {
    }

    public record Receipt(
            String hash,
            String symbol,
            long sequence,
            UUID orderId
    ) {
    }
}
