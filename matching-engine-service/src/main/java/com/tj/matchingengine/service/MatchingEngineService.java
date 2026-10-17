package com.tj.matchingengine.service;

import com.tj.common.dto.OrderBookDTO;

import java.util.Set;

public interface MatchingEngineService {

    OrderBookDTO getOrderBook(String symbol);

    Set<String> getActiveSymbols();
}
