package com.tj.portfolioledger.repository;

import com.tj.common.dto.TradeDTO;
import com.tj.portfolioledger.model.Trade;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static com.tj.portfolioledger.jooq.Tables.TRADES;

@Repository
@RequiredArgsConstructor
public class TradeRepository {
    private final DSLContext dsl;

    public void insertIntoTrades(Trade trade){
        OffsetDateTime time = OffsetDateTime.ofInstant(trade.getExecutedAt(), ZoneOffset.UTC);
        dsl.insertInto(TRADES)
                .set(TRADES.ID, trade.getId())
                .set(TRADES.BUY_ORDER_ID, trade.getBuyOrderId())
                .set(TRADES.SELL_ORDER_ID, trade.getSellOrderId())
                .set(TRADES.PRICE, trade.getPrice())
                .set(TRADES.QUANTITY, trade.getQuantity())
                .set(TRADES.SYMBOL, trade.getSymbol())
                .set(TRADES.EXECUTED_AT, time)
                .execute();
    }

    public List<TradeDTO> getTradesByUserId(UUID userId){
        return dsl.selectFrom(TRADES)
                .where(TRADES.BUY_ORDER_ID.eq(userId)).or(TRADES.SELL_ORDER_ID.eq(userId))
                .fetch()
                .map(record -> TradeDTO.builder()
                        .id(record.getId())
                        .buyOrderId(record.getBuyOrderId())
                        .sellOrderId(record.getSellOrderId())
                        .symbol(record.getSymbol())
                        .price(record.getPrice())
                        .quantity(record.getQuantity())
                        .executedAt(record.getExecutedAt().toInstant())
                        .build()
                );
    }
}
