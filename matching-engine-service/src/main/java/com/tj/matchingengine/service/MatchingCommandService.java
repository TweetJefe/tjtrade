package com.tj.matchingengine.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.event.MatchingResults;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.matchingengine.repository.MatchingRepository;
import com.tj.messaging.OrderPlacedMessageCodec;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class MatchingCommandService {

    private final MatchingRepository repository;
    private final OrderPlacedMessageCodec codec = new OrderPlacedMessageCodec();
    private final JsonMapper resultMapper = JsonMapper.builder()
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public boolean apply(String key, String json) {
        var message = codec.decode(json);
        OrderPlacedEvent order = message.payload();

        if (!order.symbol().matches("[A-Z0-9]+_[A-Z0-9]+") || order.symbol().length() > 20
                || !message.aggregateKey().equals(key)) {
            throw new IllegalArgumentException("Command market does not match the Kafka key");
        }

        String hash = codec.payloadHash(json);
        long position = repository.lockMarket(key);
        var receipt = repository.findReceipt(message.eventId());

        if (receipt.isPresent()) {
            var previous = receipt.get();

            if (!hash.equals(previous.hash()) || !key.equals(previous.symbol())
                    || message.sequence() != previous.sequence() || !order.orderId().equals(previous.orderId())
                    || previous.sequence() > position) {
                throw new IllegalStateException("Duplicate command has conflicting content or history");
            }

            repository.requireResult(message.eventId(), key, message.sequence(), order.orderId());

            return false;
        }

        if (message.sequence() != Math.addExact(position, 1L)) {
            throw new IllegalStateException("Matching command sequence is not contiguous");
        }

        repository.insertOrder(message);
        BigDecimal remaining = order.quantity();
        var fills = new ArrayList<MatchingResults.Fill>();

        for (var maker : repository.matchingMakers(order)) {
            if (remaining.signum() == 0) {
                break;
            }

            BigDecimal quantity = remaining.min(maker.remainingQuantity());
            boolean incomingBuy = order.side() == OrderSide.BUY;
            fills.add(new MatchingResults.Fill(
                    identity(message.eventId() + ":" + maker.orderId()),
                    incomingBuy ? order.orderId() : maker.orderId(),
                    incomingBuy ? maker.orderId() : order.orderId(),
                    incomingBuy ? order.userId() : maker.userId(),
                    incomingBuy ? maker.userId() : order.userId(),
                    maker.price(), quantity, message.occurredAt()
            ));
            repository.fill(maker.orderId(), quantity);
            repository.fill(order.orderId(), quantity);
            remaining = remaining.subtract(quantity);
        }

        UUID resultId = identity("matching-result:" + message.eventId());
        var result = new MatchingResults.Batch(resultId, message.eventId(), order.orderId(), key,
                message.sequence(), remaining, fills);
        repository.saveResult(resultId, message.eventId(), key, message.sequence(), resultMapper.writeValueAsString(result));
        repository.recordReceipt(message, hash);
        repository.advance(key, position);

        return true;
    }

    private UUID identity(String value) {
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }
}
