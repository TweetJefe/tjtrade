package com.tj.portfolioledger.service;

import com.tj.common.dto.OrderDTO;
import com.tj.common.dto.TradeDTO;
import com.tj.common.event.OrderPlacedEvent;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.portfolioledger.dto.PortfolioDTO;
import com.tj.portfolioledger.dto.TradeHistoryDTO;
import com.tj.portfolioledger.model.Order;
import com.tj.portfolioledger.model.Trade;
import com.tj.portfolioledger.repository.OrderRepository;
import com.tj.portfolioledger.repository.TradeRepository;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.tj.common.enums.OrderStatus;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PortfolioLedgerServiceImpl implements PortfolioLedgerService {
    private final OrderRepository orderRepository;
    private final TradeRepository tradeRepository;
    private final WalletBalanceRepository balanceRepository;

    @Override
    public PortfolioDTO getUserPortfolio(UUID userId) {
        return null;
    }

    @Override
    public TradeHistoryDTO getTradeHistory(UUID userId) {
        return null;
    }

    @Override
    public void saveOrder(OrderPlacedEvent event) {
        Order order = Order.builder()
                .id(event.orderId())
                .userId(event.userId())
                .symbol(event.symbol())
                .side(event.side())
                .type(event.type())
                .price(event.price())
                .quantity(event.quantity())
                .filledQuantity(BigDecimal.ZERO)
                .status(OrderStatus.NEW)
                .createdAt(event.timestamp())
                .updatedAt(event.timestamp())
                .build();
        orderRepository.insertIntoOrders(order);
    }

    @Override
    @Transactional
    public void saveTrade(TradeExecutedEvent event) {
        Trade trade = Trade.builder()
                .id(event.tradeId())
                .buyOrderId(event.buyOrderId())
                .sellOrderId(event.sellOrderId())
                .symbol(event.symbol())
                .price(event.price())
                .quantity(event.quantity())
                .executedAt(event.executedAt())
                .build();
        tradeRepository.insertIntoTrades(trade);

        String[] currencies = event.symbol().split("_");
        String baseAsset = currencies[0];
        String quoteAsset = currencies[1];
        BigDecimal baseAmount = event.quantity();
        BigDecimal quoteAmount = event.price().multiply(event.quantity());

        balanceRepository.settleTrade(
                event.buyerUserId(), event.sellerUserId(),
                baseAsset, quoteAsset, baseAmount, quoteAmount
        );
    }

    @Override
    public List<OrderDTO> getOrdersByUserId(UUID userId) {
        return orderRepository.getOrdersByUserId(userId);
    }

    @Override
    public List<TradeDTO> getTradesByUserId(UUID userId) {
        return tradeRepository.getTradesByUserId(userId);
    }
}
