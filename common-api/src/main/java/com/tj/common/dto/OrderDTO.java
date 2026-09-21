package com.tj.common.dto;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderStatus;
import com.tj.common.enums.OrderType;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record OrderDTO(
        UUID id,
        UUID orderId,
        UUID userId,
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal price,
        BigDecimal quantity,
        BigDecimal filledQuantity,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt
) {}
