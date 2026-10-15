package com.tj.portfolioledger.result;

import com.tj.portfolioledger.enums.AcceptOrderOutcome;

import java.util.UUID;

public record AcceptOrderResult(
        UUID orderId,
        AcceptOrderOutcome outcome
) {
}
