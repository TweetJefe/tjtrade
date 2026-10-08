package com.tj.portfolioledger.model;


import java.math.BigDecimal;
import java.util.UUID;

public record FundReservation(
        UUID orderId,
        UUID userId,
        String asset,
        BigDecimal amount,
        String status
) {
}
