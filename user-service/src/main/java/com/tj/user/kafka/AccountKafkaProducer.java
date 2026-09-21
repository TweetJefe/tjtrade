package com.tj.user.kafka;

import com.tj.common.event.BalanceChangedEvent;
import com.tj.user.model.Account;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccountKafkaProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_BALANCE_EVENTS= "balance-events-topic";

    public void sendBalanceEvent(Account account, String operationType, BigDecimal amount){
        BalanceChangedEvent event = BalanceChangedEvent.builder()
                .accountId(account.getId())
                .userId(account.getUserId())
                .currency(account.getCurrency())
                .operationType(operationType)
                .amount(amount)
                .balanceAfter(account.getBalance())
                .timestamp(Instant.now())
                .build();
        kafkaTemplate.send("balance-events-topic", account.getId().toString(), event);
        log.debug("Published BalanceChangedEvent for account {} (Operation: {})", account.getId(), operationType);
    }
}
