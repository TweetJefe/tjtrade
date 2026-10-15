package com.tj.portfolioledger.command;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;

import java.math.BigDecimal;
import java.util.UUID;

public record AcceptOrderCommand(
        UUID orderId,
        UUID userId,
        String symbol,
        OrderSide side,
        OrderType type,
        BigDecimal price,
        BigDecimal quantity
) {
}
