package com.tj.common.dto;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import lombok.Builder;

import java.math.BigDecimal;
import java.util.UUID;

@Builder
public record PlaceOrderRequest(
        UUID userId,
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal price,
        BigDecimal quantity
) {}
