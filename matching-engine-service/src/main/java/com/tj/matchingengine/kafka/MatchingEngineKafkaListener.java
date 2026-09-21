package com.tj.matchingengine.kafka;

import com.tj.common.dto.OrderDTO;
import com.tj.common.enums.OrderStatus;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.matchingengine.dto.MatchResultDTO;
import com.tj.matchingengine.service.MatchingEngineServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "orders-topic", groupId = "matching-engine-group")
public class MatchingEngineKafkaListener {

    private final MatchingEngineServiceImpl service;
    private final MatchingEngineKafkaProducer producer;

    @KafkaHandler
    public void handleOrderPlacedEvent(OrderPlacedEvent event) {
        log.info("Received OrderPlacedEvent for order: {}", event.orderId());
        try {
            OrderDTO dto = OrderDTO.builder()
                    .id(event.orderId())
                    .orderId(event.orderId())
                    .userId(event.userId())
                    .symbol(event.symbol())
                    .side(event.side())
                    .type(event.type())
                    .price(event.price())
                    .quantity(event.quantity())
                    .filledQuantity(BigDecimal.ZERO)
                    .status(OrderStatus.NEW)
                    .createdAt(event.timestamp())
                    .updatedAt(event.timestamp())
                    .build();

            List<MatchResultDTO> trades = service.processOrder(dto);

            for(MatchResultDTO trade : trades){
                producer.sendTradeExecutedEvent(trade);
            }

            producer.sendOrderBookSnapshot(service.getOrderBook(event.symbol()));
        } catch (Exception e) {
            log.error("Failed to process placed order {}: {}", event.orderId(), e.getMessage());
        }
    }
}
