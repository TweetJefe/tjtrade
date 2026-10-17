package com.tj.portfolioledger.messaging.outbox;

import com.tj.common.event.TradingMessages;
import com.tj.portfolioledger.jooq.tables.records.OutboxEventRecord;
import com.tj.portfolioledger.repository.OutboxPublishCursorRepository;
import com.tj.portfolioledger.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxRepository repository;
    private final OutboxPublishCursorRepository cursorRepository;
    private final OutboxPublicationCoordinator coordinator;
    private final OrderPlacedMessageCodec codec;
    private final FencedCommandProducerFactory producers;

    public boolean publishNext(String symbol) throws Exception {
        var published = new AtomicBoolean();

        coordinator.tryRun(TradingMessages.MATCHING_COMMANDS, symbol, cursor -> {
            long nextSequence = Math.addExact(cursor.lastPublishedSequence(), 1L);

            if (repository.lastAllocated(cursor.stream(), symbol) < nextSequence) {
                return;
            }

            // Every attempt initializes a stable transactional ID while holding the SQL lock.
            // Never reuse or recreate a fenced producer outside this transaction boundary.
            var producer = producers.open(cursor.stream(), symbol);

            boolean transactionStarted = false;

            try {
                producer.initTransactions();

                // Re-read through the pinned SQL session after fencing. A lost session must
                // fail here rather than send using a cursor captured before takeover.
                long position = cursorRepository.currentPosition(cursor.stream(), symbol);

                if (position != cursor.lastPublishedSequence()) {
                    throw new IllegalStateException("Outbox publication ownership changed");
                }

                var event = repository.findSequence(cursor.stream(), symbol, nextSequence)
                        .orElseThrow(() -> new IllegalStateException("Outbox command sequence has a gap"));
                String payload = verifiedPayload(event, cursor, nextSequence);

                producer.beginTransaction();
                transactionStarted = true;
                producer.send(new ProducerRecord<>(cursor.stream(), symbol, payload)).get(2, TimeUnit.SECONDS);
                producer.commitTransaction();
                transactionStarted = false;

                repository.markPublished(event.getEventId());
                cursorRepository.advance(cursor.stream(), symbol, position);
                published.set(true);
            } catch (Exception failure) {
                if (transactionStarted) {
                    try {
                        producer.abortTransaction();
                    } catch (Exception abortFailure) {
                        failure.addSuppressed(abortFailure);
                    }
                }

                throw failure;
            } finally {
                producer.close(Duration.ofSeconds(1));
            }
        });

        return published.get();
    }

    private String verifiedPayload(OutboxEventRecord event, OutboxPublication.Cursor cursor, long sequence) {
        var message = codec.decode(event.getPayload().data());
        String canonical = codec.encode(message);

        if (event.getPublishedAt() != null || !event.getEventId().equals(message.eventId())
                || !cursor.stream().equals(message.stream()) || !cursor.aggregateKey().equals(message.aggregateKey())
                || sequence != message.sequence() || !event.getEventType().equals(message.eventType())
                || event.getSchemaVersion() != message.schemaVersion()
                || !event.getCorrelationId().equals(message.correlationId())
                || !event.getCreatedAt().toInstant().equals(message.occurredAt())
                || !codec.payloadHash(canonical).equals(event.getPayloadHash())) {
            throw new IllegalStateException("Outbox command integrity check failed");
        }

        return canonical;
    }
}
