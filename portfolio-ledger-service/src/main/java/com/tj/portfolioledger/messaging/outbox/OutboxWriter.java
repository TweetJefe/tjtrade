package com.tj.portfolioledger.messaging.outbox;

import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.portfolioledger.exception.AcceptOrderException;
import com.tj.portfolioledger.model.Order;
import com.tj.portfolioledger.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional(propagation = Propagation.MANDATORY)
public class OutboxWriter {

    private final OutboxRepository repository;
    private final OrderPlacedMessageCodec codec;

    public void appendOrderPlaced(Order order) {
        long sequence = repository.nextSequence(TradingMessages.MATCHING_COMMANDS, order.getSymbol());
        var message = envelope(UUID.randomUUID(), sequence, order);
        String payload = codec.encode(message);
        String payloadHash = codec.payloadHash(payload);

        repository.insert(message, payload, payloadHash);
    }

    public void requireOrderPlaced(Order order) {
        var stored = repository.findOrderPlaced(order.getId())
                .orElseThrow(this::inconsistentOutbox);

        try {
            String expected = codec.encode(envelope(stored.getEventId(), stored.getSequence(), order));
            boolean consistent = order.getSymbol().equals(stored.getAggregateKey())
                    && stored.getSchemaVersion() == TradingMessages.SCHEMA_VERSION
                    && expected.equals(codec.canonicalize(stored.getPayload().data()))
                    && codec.payloadHash(expected).equals(stored.getPayloadHash());

            if (!consistent) {
                throw inconsistentOutbox();
            }
        } catch (IllegalArgumentException exception) {
            throw inconsistentOutbox();
        }
    }

    private TradingMessages.Envelope<OrderPlacedEvent> envelope(UUID eventId, long sequence, Order order) {
        var payload = new OrderPlacedEvent(
                order.getId(), order.getUserId(), order.getSymbol(), order.getSide(), order.getType(),
                order.getPrice(), order.getQuantity(), order.getCreatedAt()
        );

        return new TradingMessages.Envelope<>(
                eventId, TradingMessages.ORDER_PLACED, TradingMessages.SCHEMA_VERSION,
                TradingMessages.MATCHING_COMMANDS, order.getSymbol(), sequence,
                order.getId(), order.getCreatedAt(), payload
        );
    }

    private AcceptOrderException inconsistentOutbox() {
        return new AcceptOrderException(
                AcceptOrderException.Reason.INCONSISTENT_OUTBOX,
                "Existing order has no valid outbox command"
        );
    }
}
