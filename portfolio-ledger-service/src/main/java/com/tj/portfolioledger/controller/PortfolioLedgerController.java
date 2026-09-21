package com.tj.portfolioledger.controller;

import com.tj.common.dto.OrderDTO;
import com.tj.common.dto.TradeDTO;
import com.tj.portfolioledger.service.PortfolioLedgerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/portfolio")
@RequiredArgsConstructor
public class PortfolioLedgerController {

    private final PortfolioLedgerService portfolioLedgerService;

    @GetMapping("{userId}/orders")
    public ResponseEntity<List<OrderDTO>> getOrdersByUserId(@PathVariable UUID userId){
        return ResponseEntity.ok(portfolioLedgerService.getOrdersByUserId(userId));
    }

    @GetMapping("{userId}/trades")
    public ResponseEntity<List<TradeDTO>> getTradesByUserId(@PathVariable UUID userId){
        return ResponseEntity.ok(portfolioLedgerService.getTradesByUserId(userId));
    }

}
