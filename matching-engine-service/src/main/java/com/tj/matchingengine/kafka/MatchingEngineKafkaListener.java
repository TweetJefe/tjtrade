package com.tj.matchingengine.kafka;

import com.tj.common.event.TradingMessages;
import com.tj.matchingengine.service.MatchingCommandService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MatchingEngineKafkaListener {

    private final MatchingCommandService service;

    @KafkaListener(topics = TradingMessages.MATCHING_COMMANDS, groupId = "matching-orders-v1",
            containerFactory = "matchingCommandContainerFactory")
    public void handle(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        service.apply(record.key(), record.value());

        // The service proxy has committed SQL before the Kafka offset is acknowledged.
        acknowledgment.acknowledge();
    }
}
