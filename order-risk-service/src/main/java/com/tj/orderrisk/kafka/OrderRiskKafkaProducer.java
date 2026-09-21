package com.tj.orderrisk.kafka;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.orderrisk.dto.RiskCheckRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderRiskKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderPlacedEvent(UUID orderId, RiskCheckRequest request) {
        OrderPlacedEvent event = OrderPlacedEvent.builder()
                .orderId(orderId)
                .userId(request.getUserId())
                .symbol(request.getSymbol())
                .side(request.getSide())
                .type(request.getType())
                .price(request.getPrice())
                .quantity(request.getQuantity())
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send("orders-topic", orderId.toString(), event);
        log.info("Sent OrderPlacedEvent to Kafka for Order ID: {}", orderId);
    }
}
