package com.tj.common.event;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Builder
public record BalanceChangedEvent(
        UUID accountId,
        UUID userId,
        String currency,
        String operationType,
        BigDecimal amount,
        BigDecimal balanceAfter,
        Instant timestamp
) {}
