package com.tj.portfolioledger.messaging.outbox;

import com.tj.portfolioledger.repository.OutboxPublishCursorRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Serializes synchronous work within a database transaction.
 * Database ownership does not fence an external producer after session loss.
 * Kafka work must establish broker fencing before reading and sending commands.
 */
@Service
@RequiredArgsConstructor
public class OutboxPublicationCoordinator {

    private final OutboxPublishCursorRepository repository;

    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            isolation = Isolation.READ_COMMITTED,
            timeout = 10,
            rollbackFor = Exception.class
    )
    public boolean tryRun(String stream, String aggregateKey, OutboxPublication.Work work) throws Exception {
        validateKey(stream, 100, "Stream");
        validateKey(aggregateKey, 255, "Aggregate key");

        if (work == null) {
            throw new IllegalArgumentException("Publication work is required");
        }

        var cursor = repository.tryLock(stream, aggregateKey);

        if (cursor.isEmpty()) {
            return false;
        }

        work.execute(cursor.get());

        return true;
    }

    private void validateKey(String value, int maximumLength, String name) {
        if (value == null || value.isBlank() || value.length() > maximumLength
                || !value.equals(value.strip())) {
            throw new IllegalArgumentException(name + " is invalid");
        }
    }
}
