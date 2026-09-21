package com.tj.matchingengine.kafka;

import com.tj.common.dto.OrderBookDTO;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.matchingengine.dto.MatchResultDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MatchingEngineKafkaProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendTradeExecutedEvent(MatchResultDTO result){
        TradeExecutedEvent event = TradeExecutedEvent.builder()
                .tradeId(result.tradeId())
                .symbol(result.symbol())
                .buyerUserId(result.buyerUserId())
                .sellerUserId(result.sellerUserId())
                .price(result.price())
                .quantity(result.quantity())
                .executedAt(result.timestamp())
                .build();
                
        kafkaTemplate.send("trades-topic", result.tradeId().toString(), event);
        log.info("Trade Executed! Sent to Kafka -> Trade ID: {}", result.tradeId());
    }

    public void sendOrderBookSnapshot(OrderBookDTO snapshot){
        kafkaTemplate.send("orderbook-snapshots-topic", snapshot.symbol(), snapshot);

        log.info("Orderbook Snapshot sent to Kafka for symbol: {}", snapshot.symbol());
    }
}
