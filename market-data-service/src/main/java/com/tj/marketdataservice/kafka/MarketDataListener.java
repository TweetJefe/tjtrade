package com.tj.marketdataservice.kafka;

import com.tj.common.dto.OrderBookDTO;
import com.tj.common.dto.TradeDTO;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.marketdataservice.service.TickerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MarketDataListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final TickerService tickerService;


    @KafkaListener(topics = "orderbook-snapshots-topic", groupId = "market-data-group")
    public void consumeOrderBookSnapshot(OrderBookDTO orderBook){
        log.debug("Received snapshot for {}: bids={}, asks={}",
                orderBook.symbol(),
                orderBook.bids().size(),
                orderBook.asks().size());

        String destination = "/topic/orderbook/" + orderBook.symbol();

        messagingTemplate.convertAndSend(destination, orderBook);
    }

    @KafkaListener(topics = "trades-topic", groupId = "market-data-group")
    public void consumeTradeExecutedEvents(TradeExecutedEvent event){
        log.info("Recieved TradeExecutedEvent: symbol {}, price = {}", event.symbol(), event.price());

        tickerService.pushPrice(event.symbol(), event.price());
    }
}
