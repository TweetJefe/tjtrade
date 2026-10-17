package com.tj.matchingengine.repository;

import com.tj.common.enums.OrderSide;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradingMessages;
import com.tj.matchingengine.model.MatchingState;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class MatchingRepository {

    public static final String CONSUMER = "matching-orders-v1";

    private final JdbcTemplate jdbc;

    public long lockMarket(String symbol) {
        jdbc.update("insert into matching_market (symbol) values (?) on conflict (symbol) do nothing", symbol);

        return jdbc.queryForObject("select last_sequence from matching_market where symbol = ? for update", Long.class, symbol);
    }

    public Optional<MatchingState.Receipt> findReceipt(UUID eventId) {
        return jdbc.query("select * from matching_inbox where consumer = ? and event_id = ?",
                (result, row) -> new MatchingState.Receipt(result.getString("payload_hash"), result.getString("symbol"),
                        result.getLong("sequence"), result.getObject("order_id", UUID.class)), CONSUMER, eventId)
                .stream()
                .findFirst();
    }

    public void requireResult(UUID eventId, String symbol, long sequence, UUID orderId) {
        int count = jdbc.queryForObject("""
                select count(*) from matching_result_outbox
                where command_event_id = ? and symbol = ? and sequence = ? and schema_version = 1
                  and payload->>'orderId' = ? and payload->>'commandEventId' = ?
                """, Integer.class, eventId, symbol, sequence, orderId.toString(), eventId.toString());

        if (count != 1) {
            throw new IllegalStateException("Matching receipt has no consistent durable result");
        }
    }

    public void insertOrder(TradingMessages.Envelope<OrderPlacedEvent> message) {
        OrderPlacedEvent order = message.payload();
        requireOne(jdbc.update("""
                insert into matching_order
                    (order_id, user_id, symbol, side, price, quantity, remaining_quantity, accepted_sequence, created_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, order.orderId(), order.userId(), order.symbol(), order.side().name(), order.price(),
                order.quantity(), order.quantity(), message.sequence(), OffsetDateTime.ofInstant(order.timestamp(), ZoneOffset.UTC)));
    }

    public List<MatchingState.Order> matchingMakers(OrderPlacedEvent order) {
        String comparison = order.side() == OrderSide.BUY ? "<=" : ">=";
        String direction = order.side() == OrderSide.BUY ? "asc" : "desc";
        String opposite = order.side() == OrderSide.BUY ? OrderSide.SELL.name() : OrderSide.BUY.name();

        return jdbc.query("select * from matching_order where symbol = ? and side = ? and remaining_quantity > 0 "
                        + "and price " + comparison + " ? order by price " + direction + ", accepted_sequence asc for update",
                this::order, order.symbol(), opposite, order.price());
    }

    public void fill(UUID orderId, BigDecimal quantity) {
        requireOne(jdbc.update("""
                update matching_order set remaining_quantity = remaining_quantity - ?
                where order_id = ? and remaining_quantity >= ?
                """, quantity, orderId, quantity));
    }

    public void saveResult(UUID eventId, UUID commandEventId, String symbol, long sequence, String payload) {
        requireOne(jdbc.update("""
                insert into matching_result_outbox (event_id, command_event_id, symbol, sequence, payload)
                values (?, ?, ?, ?, cast(? as jsonb))
                """, eventId, commandEventId, symbol, sequence, payload));
    }

    public void recordReceipt(TradingMessages.Envelope<OrderPlacedEvent> message, String hash) {
        requireOne(jdbc.update("""
                insert into matching_inbox (consumer, event_id, payload_hash, symbol, sequence, order_id)
                values (?, ?, ?, ?, ?, ?)
                """, CONSUMER, message.eventId(), hash, message.aggregateKey(), message.sequence(), message.correlationId()));
    }

    public void advance(String symbol, long previousSequence) {
        requireOne(jdbc.update("update matching_market set last_sequence = ? where symbol = ? and last_sequence = ?",
                Math.addExact(previousSequence, 1L), symbol, previousSequence));
    }

    public List<MatchingState.Order> liveOrders(String symbol) {
        return jdbc.query("select * from matching_order where symbol = ? and remaining_quantity > 0 order by accepted_sequence",
                this::order, symbol);
    }

    public List<String> activeSymbols() {
        return jdbc.queryForList("select distinct symbol from matching_order where remaining_quantity > 0", String.class);
    }

    private MatchingState.Order order(ResultSet result, int row) throws SQLException {
        return new MatchingState.Order(
                result.getObject("order_id", UUID.class), result.getObject("user_id", UUID.class),
                result.getString("symbol"), OrderSide.valueOf(result.getString("side")), result.getBigDecimal("price"),
                result.getBigDecimal("remaining_quantity"), result.getLong("accepted_sequence"),
                result.getObject("created_at", OffsetDateTime.class).toInstant()
        );
    }

    private void requireOne(int updatedRows) {
        if (updatedRows != 1) {
            throw new IllegalStateException("Matching state changed unexpectedly");
        }
    }
}
