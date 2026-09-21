package com.tj.common.event;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record OrderCancelledEvent(
        UUID orderId,
        UUID userId,
        String symbol,
        BigDecimal releasedAmount,
        String currency,
        Instant timestamp
) {}
