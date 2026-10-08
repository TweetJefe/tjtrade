package com.tj.portfolioledger.result;

import com.tj.portfolioledger.enums.ReserveFundsOutcome;

import java.util.UUID;

public record ReserveFundsResult(
        UUID orderId,
        ReserveFundsOutcome outcome
) {
}
