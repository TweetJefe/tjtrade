package com.tj.orderrisk.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RiskCheckResponse {
    private java.util.UUID orderId;
    private String outcome;
    private boolean approved;
    private String rejectReason;
}
