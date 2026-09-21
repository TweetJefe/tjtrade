package com.tj.user.service;

import com.tj.common.dto.AccountDTO;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.user.dto.CreateAccountRequest;
import com.tj.user.dto.DepositRequest;
import com.tj.user.dto.WithdrawalRequest;

import java.math.BigDecimal;
import java.util.UUID;

public interface AccountService {
    AccountDTO createAccount(CreateAccountRequest request);

    BigDecimal getBalanceByCurrency(UUID userId, String currency);

    BigDecimal getBalanceByAccountId(UUID accountId);

    AccountDTO depositFunds(UUID accountId, BigDecimal amount);

    AccountDTO withdrawFunds(UUID accountId, WithdrawalRequest request);

    void unreserveFunds(UUID userId, String currency, BigDecimal amount);

    void settleTrade(TradeExecutedEvent event);

    void reserveFunds(UUID userId, String currency, BigDecimal amount);
}
