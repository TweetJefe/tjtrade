package com.tj.portfolioledger.model;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderStatus;
import com.tj.common.enums.OrderType;
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
public class Order {
    private UUID id;
    private UUID userId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal filledQuantity;
    private OrderStatus status;
    private Instant createdAt;
    private Instant updatedAt;
}
