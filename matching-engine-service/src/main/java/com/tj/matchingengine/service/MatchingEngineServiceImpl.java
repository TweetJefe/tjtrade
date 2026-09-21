package com.tj.matchingengine.service;

import com.tj.common.dto.OrderBookDTO;
import com.tj.common.dto.OrderBookLevel;
import com.tj.common.dto.OrderDTO;
import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.matchingengine.dto.MatchResultDTO;
import com.tj.matchingengine.model.OrderBook;
import com.tj.matchingengine.model.OrderBookItem;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;

@Service
@RequiredArgsConstructor
public class MatchingEngineServiceImpl implements MatchingEngineService {

    private final Map<String, OrderBook> orderBooks = new ConcurrentHashMap<>();

    @Override
    public List<MatchResultDTO> processOrder(OrderDTO orderDTO) {
        List<MatchResultDTO> results = new ArrayList<>();

        OrderBook book = orderBooks.computeIfAbsent(orderDTO.symbol(), s -> OrderBook.builder().symbol(s).build());

        OrderBookItem incomingOrder = OrderBookItem.builder()
                .orderId(orderDTO.orderId())
                .userId(orderDTO.userId())
                .side(orderDTO.side())
                .price(orderDTO.price())
                .remainingQuantity(orderDTO.quantity())
                .timestamp(java.time.Instant.now())
                .build();

        ConcurrentNavigableMap<BigDecimal, Queue<OrderBookItem>> oppositeBook =
                (incomingOrder.getSide() == OrderSide.BUY) ? book.getAsks() : book.getBids();

        while (incomingOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && !oppositeBook.isEmpty()){
            Map.Entry<BigDecimal, Queue<OrderBookItem>> bestLevel = oppositeBook.firstEntry();
            BigDecimal bestPrice = bestLevel.getKey();
            Queue<OrderBookItem> bestQueue = bestLevel.getValue();

            if(orderDTO.type() == OrderType.LIMIT){
                if(incomingOrder.getSide() == OrderSide.BUY){
                    if (incomingOrder.getPrice().compareTo(bestPrice) < 0){
                        break;
                    }
                }else{
                    if (incomingOrder.getPrice().compareTo(bestPrice) > 0){
                        break;
                    }
                }
            }

            OrderBookItem makerOrder = bestQueue.peek();
            if (makerOrder == null){
                oppositeBook.remove(bestPrice);
                continue;
            }

            BigDecimal tradeQuantity = incomingOrder.getRemainingQuantity().min(makerOrder.getRemainingQuantity());

            incomingOrder.setRemainingQuantity(
                    incomingOrder.getRemainingQuantity().subtract(tradeQuantity)
            );

            makerOrder.setRemainingQuantity(
                    makerOrder.getRemainingQuantity().subtract(tradeQuantity)
            );

            UUID buyerId = (incomingOrder.getSide() == OrderSide.BUY) ? incomingOrder.getUserId() : makerOrder.getUserId();
            UUID sellerId = (incomingOrder.getSide() == OrderSide.BUY) ? makerOrder.getUserId() : incomingOrder.getUserId();
            MatchResultDTO resultDTO = MatchResultDTO.builder()
                    .tradeId(UUID.randomUUID())
                    .symbol(orderDTO.symbol())
                    .makerOrderId(makerOrder.getOrderId())
                    .takerOrderId(incomingOrder.getOrderId())
                    .buyerUserId(buyerId)
                    .sellerUserId(sellerId)
                    .price(makerOrder.getPrice())
                    .quantity(tradeQuantity)
                    .timestamp(java.time.Instant.now())
                    .build();
            results.add(resultDTO);

            if(makerOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) == 0){
                bestQueue.poll();
            }

            if (bestQueue.isEmpty()){
                oppositeBook.remove(bestPrice);
            }
        }
        if(incomingOrder.getRemainingQuantity().compareTo(BigDecimal.ZERO) > 0 && orderDTO.type() == OrderType.LIMIT){
            ConcurrentNavigableMap<BigDecimal, Queue<OrderBookItem>> myBook =
                    (incomingOrder.getSide() == OrderSide.BUY) ? book.getBids() : book.getAsks();

            myBook.computeIfAbsent(incomingOrder.getPrice(), k -> new ConcurrentLinkedQueue<>())
                    .add(incomingOrder);
        }

        return results;
    }

    @Override
    public OrderBookDTO getOrderBook(String symbol) {
        OrderBook book = orderBooks.get(symbol);
        if (book == null){
            return new OrderBookDTO(symbol, List.of(), List.of());
        }

        List<OrderBookLevel> bids = book.getBids().entrySet()
                .stream().map(entry -> new OrderBookLevel(
                        entry.getKey(),
                        entry.getValue().stream()
                                .map(OrderBookItem::getRemainingQuantity)
                                .reduce(BigDecimal.ZERO, BigDecimal::add)
                )).toList();

        List<OrderBookLevel> asks = book.getAsks().entrySet()
                .stream().map(entry -> new OrderBookLevel(
                        entry.getKey(),
                        entry.getValue().stream()
                                .map(OrderBookItem::getRemainingQuantity)
                                .reduce(BigDecimal.ZERO, BigDecimal::add)
                )).toList();

        return new OrderBookDTO(symbol, bids, asks);
    }

    @Override
    public Set<String> getActiveSymbols() {
        return orderBooks.keySet();
    }
}
