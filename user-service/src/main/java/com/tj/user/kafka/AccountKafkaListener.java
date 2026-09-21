package com.tj.user.kafka;

import com.tj.common.enums.OrderSide;
import com.tj.common.event.OrderCancelledEvent;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.user.service.AccountService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
@KafkaListener(topics = {"orders-topic", "trades-topic"}, groupId = "user-service-group")
public class AccountKafkaListener {
    private final AccountService accountService;


    @KafkaHandler
    public void handleOrderCancelled(OrderCancelledEvent event) {
        log.info("Received OrderCancelledEvent for user {} and currency {}. Releasing {} amount",
                event.userId(), event.currency(), event.releasedAmount());
        try {
            accountService.unreserveFunds(event.userId(), event.currency(), event.releasedAmount());
        } catch (Exception e) {
            log.error("Error unreserving funds for order {}: {}", event.orderId(), e.getMessage());
        }
    }

    @KafkaHandler
    public void handleTradeExecuted(TradeExecutedEvent event) {
        log.info("Received TradeExecutedEvent: Trade ID {}, Pair {}, Price {}, Qty {}",
                event.tradeId(), event.symbol(), event.price(), event.quantity());
        try {
            accountService.settleTrade(event);
        } catch (Exception e) {
            log.error("Error settling trade {}: {}", event.tradeId(), e.getMessage());
        }
    }

    @KafkaHandler
    public void handleOrderPlaced(OrderPlacedEvent event){
        log.info("Received OrderPlacedEvent for user {}, symbol {}, side {}",
                event.userId(), event.symbol(), event.side());

        String[] currencies = event.symbol().split("_");
        String baseCurrency = currencies[0];
        String quoteCurrency = currencies[1];

        String reserveCurrency;
        BigDecimal reserveAmount;

        if(event.side() == OrderSide.BUY){
            reserveCurrency = quoteCurrency;
            reserveAmount = event.price().multiply(event.quantity());
        }else{
            reserveCurrency = baseCurrency;
            reserveAmount = event.quantity();
        }

        try{
            accountService.reserveFunds(event.userId(), reserveCurrency, reserveAmount);
        }catch (Exception e){
            log.error("Error reserving funds for order {}: {}", event.orderId(), e.getMessage());
        }
    }
}
