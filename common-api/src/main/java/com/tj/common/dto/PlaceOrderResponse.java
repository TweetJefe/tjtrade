package com.tj.common.dto;

import com.tj.common.enums.OrderStatus;
import lombok.Builder;

import java.util.UUID;

@Builder
public record PlaceOrderResponse(
        UUID orderId,
        OrderStatus status,
        String rejectReason
) {}
