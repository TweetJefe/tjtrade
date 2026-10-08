package com.tj.portfolioledger.command;

import java.math.BigDecimal;
import java.util.UUID;

public record ReserveFundsCommand(
        UUID orderId,
        UUID userId,
        String asset,
        BigDecimal amount
) {
}
