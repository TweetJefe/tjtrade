package com.tj.portfolioledger.repository;

import com.tj.portfolioledger.messaging.outbox.OutboxPublication;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import static com.tj.portfolioledger.jooq.Tables.OUTBOX_PUBLISH_CURSOR;

@Repository
@RequiredArgsConstructor
@Transactional(propagation = Propagation.MANDATORY)
public class OutboxPublishCursorRepository {

    private final DSLContext dsl;

    public void initializeIfAbsent(String stream, String aggregateKey) {
        dsl.insertInto(OUTBOX_PUBLISH_CURSOR)
                .set(OUTBOX_PUBLISH_CURSOR.STREAM, stream)
                .set(OUTBOX_PUBLISH_CURSOR.AGGREGATE_KEY, aggregateKey)
                .set(OUTBOX_PUBLISH_CURSOR.LAST_PUBLISHED_SEQUENCE, 0L)
                .onConflict(OUTBOX_PUBLISH_CURSOR.STREAM, OUTBOX_PUBLISH_CURSOR.AGGREGATE_KEY)
                .doNothing()
                .execute();
    }

    public Optional<OutboxPublication.Cursor> tryLock(String stream, String aggregateKey) {
        return dsl.selectFrom(OUTBOX_PUBLISH_CURSOR)
                .where(OUTBOX_PUBLISH_CURSOR.STREAM.eq(stream))
                .and(OUTBOX_PUBLISH_CURSOR.AGGREGATE_KEY.eq(aggregateKey))
                .forUpdate()
                .skipLocked()
                .fetchOptional(record -> new OutboxPublication.Cursor(
                        record.getStream(), record.getAggregateKey(), record.getLastPublishedSequence()
                ));
    }
    public long currentPosition(String stream, String aggregateKey) {
        return dsl.select(OUTBOX_PUBLISH_CURSOR.LAST_PUBLISHED_SEQUENCE)
                .from(OUTBOX_PUBLISH_CURSOR)
                .where(OUTBOX_PUBLISH_CURSOR.STREAM.eq(stream))
                .and(OUTBOX_PUBLISH_CURSOR.AGGREGATE_KEY.eq(aggregateKey))
                .fetchSingle(OUTBOX_PUBLISH_CURSOR.LAST_PUBLISHED_SEQUENCE);
    }

    public void advance(String stream, String aggregateKey, long previousSequence) {
        int updatedRows = dsl.update(OUTBOX_PUBLISH_CURSOR)
                .set(OUTBOX_PUBLISH_CURSOR.LAST_PUBLISHED_SEQUENCE, Math.addExact(previousSequence, 1L))
                .where(OUTBOX_PUBLISH_CURSOR.STREAM.eq(stream))
                .and(OUTBOX_PUBLISH_CURSOR.AGGREGATE_KEY.eq(aggregateKey))
                .and(OUTBOX_PUBLISH_CURSOR.LAST_PUBLISHED_SEQUENCE.eq(previousSequence))
                .execute();

        if (updatedRows != 1) {
            throw new IllegalStateException("Outbox publication cursor changed unexpectedly");
        }
    }
}
