package com.tj.orderrisk.model;

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
public class OrderRiskCheck {
    private UUID id;
    private UUID userId;
    private String symbol;
    private BigDecimal requiredAmount;
    private boolean passed;
    private String rejectReason;
    private Instant createdAt;
}
