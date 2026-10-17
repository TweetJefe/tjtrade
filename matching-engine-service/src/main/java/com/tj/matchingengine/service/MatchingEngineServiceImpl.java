package com.tj.matchingengine.service;

import com.tj.common.dto.OrderBookDTO;
import com.tj.common.dto.OrderBookLevel;
import com.tj.common.enums.OrderSide;
import com.tj.matchingengine.repository.MatchingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

@Service
@RequiredArgsConstructor
public class MatchingEngineServiceImpl implements MatchingEngineService {

    private final MatchingRepository repository;

    @Override
    public OrderBookDTO getOrderBook(String symbol) {
        var bids = new TreeMap<BigDecimal, BigDecimal>(java.util.Comparator.reverseOrder());
        var asks = new TreeMap<BigDecimal, BigDecimal>();

        for (var order : repository.liveOrders(symbol)) {
            var levels = order.side() == OrderSide.BUY ? bids : asks;
            levels.merge(order.price(), order.remainingQuantity(), BigDecimal::add);
        }

        List<OrderBookLevel> bidLevels = bids.entrySet().stream()
                .map(entry -> new OrderBookLevel(entry.getKey(), entry.getValue()))
                .toList();
        List<OrderBookLevel> askLevels = asks.entrySet().stream()
                .map(entry -> new OrderBookLevel(entry.getKey(), entry.getValue()))
                .toList();

        return new OrderBookDTO(symbol, bidLevels, askLevels);
    }

    @Override
    public Set<String> getActiveSymbols() {
        return new HashSet<>(repository.activeSymbols());
    }
}
