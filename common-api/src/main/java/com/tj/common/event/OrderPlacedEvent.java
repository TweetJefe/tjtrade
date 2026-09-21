package com.tj.common.event;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record OrderPlacedEvent(
        UUID orderId,
        UUID userId,
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal price,
        BigDecimal quantity,
        Instant timestamp
) {}
