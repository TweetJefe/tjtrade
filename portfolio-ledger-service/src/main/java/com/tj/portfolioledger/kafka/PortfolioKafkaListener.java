package com.tj.portfolioledger.kafka;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.portfolioledger.service.PortfolioLedgerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PortfolioKafkaListener {
    private final PortfolioLedgerService service;

    @KafkaListener(topics = "orders-topic", groupId = "portfolio-service-group")
    public void handleOrderPlaced(OrderPlacedEvent event){
        log.info("Order {} was placed", event.orderId());
        try{
            service.saveOrder(event);
        }catch (Exception e){
            log.error("An error was accused while placing order {}", event.orderId());
        }
    }

    @KafkaListener(topics = "trades-topic", groupId = "portfolio-service-group")
    public void handleTradeExecuted(TradeExecutedEvent event){
        log.info("Trade {} between {} and {} was executed", event.tradeId(), event.buyerUserId(), event.sellerUserId());
        try{
            service.saveTrade(event);
        }catch (Exception e){
            log.info("Trade {} was not finished", event.tradeId());
        }
    }
}
