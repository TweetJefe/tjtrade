package com.tj.orderrisk.controller;

import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.dto.RiskCheckResponse;
import com.tj.orderrisk.service.OrderRiskService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/risk")
@RequiredArgsConstructor
public class OrderRiskController {

    private final OrderRiskService orderRiskService;

    @PostMapping
    public ResponseEntity<RiskCheckResponse> placeOrder(
            @RequestBody RiskCheckRequest request
    ){
        RiskCheckResponse response = orderRiskService.validateOrderRisk(request);

        if (response.isApproved()){
            return ResponseEntity.ok(response);
        }else{
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
    }

}
