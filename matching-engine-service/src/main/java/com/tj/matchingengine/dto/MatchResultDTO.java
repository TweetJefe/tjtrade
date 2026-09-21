package com.tj.matchingengine.dto;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record MatchResultDTO(
        UUID tradeId,
        String symbol,
        UUID makerOrderId,
        UUID takerOrderId,
        UUID buyerUserId,
        UUID sellerUserId,
        BigDecimal price,
        BigDecimal quantity,
        Instant timestamp
) {
}
