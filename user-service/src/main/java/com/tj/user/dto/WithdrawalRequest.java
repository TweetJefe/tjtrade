package com.tj.user.dto;

import java.math.BigDecimal;

public record WithdrawalRequest(
        BigDecimal amount,
        String destination
) {
}
