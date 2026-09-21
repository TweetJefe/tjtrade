package com.tj.common.dto;

import lombok.Builder;
import java.math.BigDecimal;
import java.util.UUID;

@Builder
public record AccountDTO(
        UUID id,
        UUID userId,
        String currency,
        BigDecimal balance,
        BigDecimal reservedBalance
) {}
