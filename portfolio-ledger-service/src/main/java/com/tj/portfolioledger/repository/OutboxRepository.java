package com.tj.portfolioledger.repository;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.portfolioledger.jooq.tables.records.OutboxEventRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static com.tj.portfolioledger.jooq.Tables.OUTBOX_EVENT;
import static com.tj.portfolioledger.jooq.Tables.OUTBOX_SEQUENCE;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {

    private final DSLContext dsl;

    public long nextSequence(String stream, String aggregateKey) {
        return dsl.insertInto(OUTBOX_SEQUENCE)
                .set(OUTBOX_SEQUENCE.STREAM, stream)
                .set(OUTBOX_SEQUENCE.AGGREGATE_KEY, aggregateKey)
                .set(OUTBOX_SEQUENCE.LAST_SEQUENCE, 1L)
                .onConflict(OUTBOX_SEQUENCE.STREAM, OUTBOX_SEQUENCE.AGGREGATE_KEY)
                .doUpdate()
                .set(OUTBOX_SEQUENCE.LAST_SEQUENCE, OUTBOX_SEQUENCE.LAST_SEQUENCE.plus(1L))
                .returningResult(OUTBOX_SEQUENCE.LAST_SEQUENCE)
                .fetchSingle()
                .value1();
    }

    public void insert(TradingMessages.Envelope<OrderPlacedEvent> message, String payload, String payloadHash) {
        int insertedRows = dsl.insertInto(OUTBOX_EVENT)
                .set(OUTBOX_EVENT.EVENT_ID, message.eventId())
                .set(OUTBOX_EVENT.STREAM, message.stream())
                .set(OUTBOX_EVENT.AGGREGATE_KEY, message.aggregateKey())
                .set(OUTBOX_EVENT.SEQUENCE, message.sequence())
                .set(OUTBOX_EVENT.EVENT_TYPE, message.eventType())
                .set(OUTBOX_EVENT.SCHEMA_VERSION, message.schemaVersion())
                .set(OUTBOX_EVENT.CORRELATION_ID, message.correlationId())
                .set(OUTBOX_EVENT.PAYLOAD, JSONB.jsonb(payload))
                .set(OUTBOX_EVENT.PAYLOAD_HASH, payloadHash)
                .set(OUTBOX_EVENT.CREATED_AT, OffsetDateTime.ofInstant(message.occurredAt(), ZoneOffset.UTC))
                .execute();

        if (insertedRows != 1) {
            throw new IllegalStateException("Outbox event was not inserted");
        }
    }

    public Optional<OutboxEventRecord> findOrderPlaced(UUID orderId) {
        return dsl.selectFrom(OUTBOX_EVENT)
                .where(OUTBOX_EVENT.STREAM.eq(TradingMessages.MATCHING_COMMANDS))
                .and(OUTBOX_EVENT.EVENT_TYPE.eq(TradingMessages.ORDER_PLACED))
                .and(OUTBOX_EVENT.CORRELATION_ID.eq(orderId))
                .fetchOptional();
    }
}
