package com.tj.portfolioledger.repository;

import com.tj.common.dto.OrderDTO;
import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderStatus;
import com.tj.common.enums.OrderType;
import com.tj.portfolioledger.model.Order;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static com.tj.portfolioledger.jooq.Tables.ORDERS;

@Repository
@RequiredArgsConstructor
public class OrderRepository {
    private final DSLContext dsl;

    public void insertIntoOrders(Order order){
        OffsetDateTime time = OffsetDateTime.ofInstant(order.getCreatedAt(), ZoneOffset.UTC);
        dsl.insertInto(ORDERS)
                .set(ORDERS.ID, order.getId())
                .set(ORDERS.USER_ID, order.getUserId())
                .set(ORDERS.SYMBOL, order.getSymbol())
                .set(ORDERS.SIDE, order.getSide().name())
                .set(ORDERS.TYPE, order.getType().name())
                .set(ORDERS.PRICE, order.getPrice())
                .set(ORDERS.QUANTITY, order.getQuantity())
                .set(ORDERS.FILLED_QUANTITY, BigDecimal.ZERO)
                .set(ORDERS.STATUS, "OPEN")
                .set(ORDERS.CREATED_AT, time)
                .set(ORDERS.UPDATED_AT, time)
                .execute();
    }

    public List<OrderDTO> getOrdersByUserId(UUID userId){
        return dsl.selectFrom(ORDERS)
                .where(ORDERS.USER_ID.eq(userId))
                .fetch()
                .map(record -> OrderDTO.builder()
                        .orderId(record.getId())
                        .userId(record.getUserId())
                        .quantity(record.getQuantity())
                        .symbol(record.getSymbol())
                        .side(OrderSide.valueOf(record.getSide()))
                        .type(OrderType.valueOf(record.getType()))
                        .price(record.getPrice())
                        .filledQuantity(record.getFilledQuantity())
                        .status(OrderStatus.valueOf(record.getStatus()))
                        .createdAt(record.getCreatedAt().toInstant())
                        .updatedAt(record.getUpdatedAt().toInstant())
                        .build());
    }
}
