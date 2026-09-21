package com.tj.matchingengine.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderBook {
    private String symbol;
    
    @Builder.Default
    private ConcurrentNavigableMap<BigDecimal, Queue<OrderBookItem>> bids = new ConcurrentSkipListMap<>((a, b) -> b.compareTo(a));

    @Builder.Default
    private ConcurrentNavigableMap<BigDecimal, Queue<OrderBookItem>> asks = new ConcurrentSkipListMap<>();

}
