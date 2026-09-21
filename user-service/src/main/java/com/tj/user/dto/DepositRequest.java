package com.tj.user.dto;

import java.math.BigDecimal;

public record DepositRequest(
        BigDecimal amount
) {
}
