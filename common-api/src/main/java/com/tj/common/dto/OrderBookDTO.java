package com.tj.common.dto;

import java.util.List;

public record OrderBookDTO(
        String symbol,
        List<OrderBookLevel> bids,
        List<OrderBookLevel> asks
) {
}
