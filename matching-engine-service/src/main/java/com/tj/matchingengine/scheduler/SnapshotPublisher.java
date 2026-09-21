package com.tj.matchingengine.scheduler;


import com.tj.matchingengine.service.MatchingEngineService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.tj.common.dto.OrderBookDTO;

import java.util.Set;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class SnapshotPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MatchingEngineService matchingEngineService;

    @Scheduled(fixedRate = 500)
    public void publishOrderBookSnapshots(){
        Set<String> symbols = matchingEngineService.getActiveSymbols();

        for (String symbol : symbols){
            OrderBookDTO snapshot = matchingEngineService.getOrderBook(symbol);

            kafkaTemplate.send("orderbook-snapshots-topic", symbol, snapshot);
        }
    }
}
