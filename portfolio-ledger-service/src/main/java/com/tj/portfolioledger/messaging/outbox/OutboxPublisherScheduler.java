package com.tj.portfolioledger.messaging.outbox;

import com.tj.portfolioledger.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "tjtrade.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisherScheduler {

    private final OutboxRepository repository;
    private final OutboxPublisher publisher;
    private final ConcurrentHashMap<String, Retry> retries = new ConcurrentHashMap<>();

    @Scheduled(fixedDelayString = "${tjtrade.outbox.poll-delay-ms:250}")
    public void publishPendingCommands() {
        for (String symbol : repository.pendingMarkets(100)) {
            Retry retry = retries.get(symbol);

            if (retry != null && Instant.now().isBefore(retry.nextAttemptAt())) {
                continue;
            }

            try {
                publisher.publishNext(symbol);
                retries.remove(symbol);
            } catch (Exception exception) {
                int attempt = retry == null ? 1 : Math.min(retry.attempt() + 1, 6);
                long delaySeconds = Math.min(1L << (attempt - 1), 30L);
                retries.put(symbol, new Retry(attempt, Instant.now().plusSeconds(delaySeconds)));
                log.error("Outbox publication failed for market {}; retry in {} seconds", symbol, delaySeconds, exception);
            }
        }
    }

    private record Retry(
            int attempt,
            Instant nextAttemptAt
    ) {
    }
}
