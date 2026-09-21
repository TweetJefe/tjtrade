package com.tj.matchingengine.service;

import com.tj.common.dto.OrderBookDTO;
import com.tj.common.dto.OrderDTO;
import com.tj.matchingengine.dto.MatchResultDTO;

import java.util.List;
import java.util.Set;

public interface MatchingEngineService {
    List<MatchResultDTO> processOrder(OrderDTO orderDTO);

    OrderBookDTO getOrderBook(String symbol);

    Set<String> getActiveSymbols();
}
