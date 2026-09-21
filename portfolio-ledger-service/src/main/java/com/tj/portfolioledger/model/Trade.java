package com.tj.portfolioledger.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Trade {
    private UUID id;
    private UUID buyOrderId;
    private UUID sellOrderId;
    private String symbol;
    private BigDecimal price;
    private BigDecimal quantity;
    private Instant executedAt;
}
