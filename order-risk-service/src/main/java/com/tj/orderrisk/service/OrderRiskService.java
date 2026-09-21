package com.tj.orderrisk.service;

import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.dto.RiskCheckResponse;

import java.math.BigDecimal;
import java.util.UUID;

public interface OrderRiskService {
    RiskCheckResponse validateOrderRisk(RiskCheckRequest request);


}
