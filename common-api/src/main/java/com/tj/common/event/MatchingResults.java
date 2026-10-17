package com.tj.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class MatchingResults {

    private MatchingResults() {
    }

    public record Batch(
            UUID eventId,
            UUID commandEventId,
            UUID orderId,
            String symbol,
            long sequence,
            BigDecimal remainingQuantity,
            List<Fill> fills
    ) {
        public Batch {
            fills = List.copyOf(fills);
        }
    }

    public record Fill(
            UUID tradeId,
            UUID buyOrderId,
            UUID sellOrderId,
            UUID buyerUserId,
            UUID sellerUserId,
            BigDecimal price,
            BigDecimal quantity,
            Instant executedAt
    ) {
    }
}
