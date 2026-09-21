package com.tj.matchingengine.model;

import com.tj.common.enums.OrderSide;
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
public class OrderBookItem {
    private UUID orderId;
    private UUID userId;
    private OrderSide side;
    private BigDecimal price;
    private BigDecimal remainingQuantity;
    private Instant timestamp;
}
