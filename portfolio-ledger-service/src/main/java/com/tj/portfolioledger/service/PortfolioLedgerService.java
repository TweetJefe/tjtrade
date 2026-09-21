package com.tj.portfolioledger.service;

import com.tj.common.dto.OrderDTO;
import com.tj.common.dto.TradeDTO;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.portfolioledger.dto.PortfolioDTO;
import com.tj.portfolioledger.dto.TradeHistoryDTO;

import java.util.List;
import java.util.UUID;

public interface PortfolioLedgerService {
    PortfolioDTO getUserPortfolio(UUID userId);

    TradeHistoryDTO getTradeHistory(UUID userId);

    void saveOrder (OrderPlacedEvent event);

    void saveTrade(TradeExecutedEvent event);

    List<OrderDTO> getOrdersByUserId(UUID userId);

    List<TradeDTO> getTradesByUserId(UUID userId);
}
