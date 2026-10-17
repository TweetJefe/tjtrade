package com.tj.orderrisk.dto;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RiskCheckRequest {
    @NotNull
    private UUID orderId;
    @NotNull
    private UUID userId;
    @NotNull
    @Size(max = 20)
    @Pattern(regexp = "[A-Z0-9]+_[A-Z0-9]+")
    private String symbol;
    @NotNull
    private OrderSide side;
    @NotNull
    private OrderType type;
    @NotNull
    @Positive
    private BigDecimal price;
    @NotNull
    @Positive
    private BigDecimal quantity;
}
