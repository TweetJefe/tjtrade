package com.tj.orderrisk.kafka;


import com.tj.common.event.BalanceChangedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderRiskKafkaListener {
    private final RedisTemplate<String, Object> redisTemplate;

    @KafkaListener(topics = "balance-events-topic", groupId = "order-risk-service-group")
    public void handleBalanceChanged(BalanceChangedEvent event){
        log.info("Received balance update for user {} and currency {}: new balance {}",
                event.userId(), event.currency(), event.balanceAfter());

        String redisKey = "balance:" + event.userId() + ":" + event.currency();

        redisTemplate.opsForValue().set(redisKey, event.balanceAfter().toString());

        log.debug("Saved to Redis -> Key: {}, Value: {}", redisKey, event.balanceAfter());
    }
}
