package com.tj.common.dto;

import java.math.BigDecimal;

public record OrderBookLevel(
        BigDecimal price,
        BigDecimal quantity
) {
}
