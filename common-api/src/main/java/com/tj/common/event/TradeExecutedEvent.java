package com.tj.common.event;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record TradeExecutedEvent(
        UUID tradeId,
        UUID buyOrderId,
        UUID sellOrderId,
        UUID buyerUserId,
        UUID sellerUserId,
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        Instant executedAt
) {}
