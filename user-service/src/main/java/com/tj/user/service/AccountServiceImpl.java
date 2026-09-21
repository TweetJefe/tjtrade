package com.tj.user.service;

import com.tj.common.dto.AccountDTO;
import com.tj.common.event.TradeExecutedEvent;
import com.tj.user.dto.CreateAccountRequest;
import com.tj.user.dto.WithdrawalRequest;
import com.tj.user.exception.AccountNotFoundException;
import com.tj.user.exception.UserNotFoundException;
import com.tj.user.kafka.AccountKafkaProducer;
import com.tj.user.model.Account;
import com.tj.user.repository.AccountRepository;
import com.tj.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountServiceImpl implements AccountService{
    private final AccountRepository accountRepository;
    private final UserRepository userRepository;
    private final AccountKafkaProducer kafkaProducer;

    @Override
    public AccountDTO createAccount(CreateAccountRequest request){
        if (!userRepository.existsById(request.getUserId())){
            throw new UserNotFoundException("User not found " + request.getUserId());
        }

        if (accountRepository.existsByIdAndCurrency(request.getUserId(), request.getCurrency())){
            throw new RuntimeException("Account for selected currency already exists " + request.getCurrency());
        }

        Account savedAccount = accountRepository.save(request.getUserId(), request.getCurrency());

        return AccountDTO.builder()
                .id(savedAccount.getId())
                .userId(savedAccount.getUserId())
                .currency(savedAccount.getCurrency())
                .balance(savedAccount.getBalance())
                .reservedBalance(savedAccount.getReservedBalance())
                .build();
    }

    @Override
    public BigDecimal getBalanceByCurrency(UUID userId, String currency) {
        return accountRepository.getBalanceByCurrency(userId, currency)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Account not found for user " + userId + "and currency" + currency
                ));
    }

    @Override
    public BigDecimal getBalanceByAccountId(UUID accountId) {
        return accountRepository.getBalanceByAccountId(accountId)
                .orElseThrow(() -> new AccountNotFoundException("Account with " + accountId + "was not found"));
    }


    @Override
    @Transactional
    public AccountDTO depositFunds(UUID accountId, BigDecimal amount) {
        if(amount == null || amount.compareTo(BigDecimal.ZERO) <= 0){
            throw new IllegalArgumentException("Deposit amount has to be greater than zero!");
        }

        Account account = accountRepository.depositFunds(accountId, amount)
                .orElseThrow(() -> new AccountNotFoundException("Account with " + accountId + "was not found"));

        kafkaProducer.sendBalanceEvent(account, "DEPOSIT", amount);

        return AccountDTO.builder()
                .id(account.getId())
                .userId(account.getUserId())
                .currency(account.getCurrency())
                .balance(account.getBalance())
                .reservedBalance(account.getReservedBalance())
                .build();
    }


    @Override
    @Transactional
    public AccountDTO withdrawFunds(UUID accountId, WithdrawalRequest request) {

        if(request.amount() == null || request.amount().compareTo(BigDecimal.ZERO) <= 0){
            throw new IllegalArgumentException("Withdrawal amount has to be greater than zero!");
        }

        Account account = accountRepository.withdrawFunds(accountId, request.amount())
                .orElseThrow(() -> new AccountNotFoundException("Account with " + accountId + "was not found"));

        kafkaProducer.sendBalanceEvent(account, "WITHDRAWAL", request.amount());

        return AccountDTO.builder()
                .id(account.getId())
                .userId(account.getUserId())
                .currency(account.getCurrency())
                .balance(account.getBalance())
                .reservedBalance(account.getReservedBalance())
                .build();
    }

    @Override
    @Transactional
    public void unreserveFunds(UUID userId, String currency, BigDecimal amount) {
        Account account = accountRepository.unreserveFunds(userId, currency, amount)
                .orElseThrow(() -> new RuntimeException("Account not found or invalid balance for unreserve"));

        kafkaProducer.sendBalanceEvent(account, "UNRESERVE", amount);
        log.info("Unreserved {} {} for user {}", amount, currency, userId);
    }

    @Override
    @Transactional
    public void settleTrade(TradeExecutedEvent event) {
        String[] currencies = event.symbol().split("_");
        String baseCurrency = currencies[0];
        String quoteCurrency = currencies[1];

        BigDecimal totalCost = event.price().multiply(event.quantity());

        Account buyerDeducted = accountRepository.deductFunds(event.buyerUserId(), quoteCurrency, totalCost)
                .orElseThrow(() -> new RuntimeException("Failed to deduct funds from buyer"));
        kafkaProducer.sendBalanceEvent(buyerDeducted, "SETTLEMENT", totalCost);

        Account buyerDeposited = accountRepository.depositFundsByUserId(event.buyerUserId(), baseCurrency, event.quantity())
                .orElseThrow(() -> new RuntimeException("Failed to deposit funds to buyer's account"));
        kafkaProducer.sendBalanceEvent(buyerDeposited, "SETTLEMENT", event.quantity());

        Account sellerDeducted = accountRepository.deductFunds(event.sellerUserId(), baseCurrency, event.quantity())
                .orElseThrow(() -> new RuntimeException("Failed to deduct funds from seller"));
        kafkaProducer.sendBalanceEvent(sellerDeducted, "SETTLEMENT", event.quantity());

        Account sellerDeposited = accountRepository.depositFundsByUserId(event.sellerUserId(), quoteCurrency, totalCost)
                .orElseThrow(() -> new RuntimeException("Failed to deposit funds to seller's account"));
        kafkaProducer.sendBalanceEvent(sellerDeposited, "SETTLEMENT", totalCost);

        log.info("Trade {} settled successfully for symbol {}", event.tradeId(), event.symbol());
    }

    @Override
    @Transactional
    public void reserveFunds(UUID userId, String currency, BigDecimal amount) {
        Account account = accountRepository.reserveFunds(userId, currency, amount)
                .orElseThrow(() -> new RuntimeException("Insufficient funds or account not found for reservation"));

        kafkaProducer.sendBalanceEvent(account, "RESERVE", amount);
        log.info("Reserved {} {} for user {}", amount, currency, userId);
    }

    private boolean isFiatCurrency(String currency){
        return Set.of("RUB", "EUR", "USD").contains(currency.toUpperCase());
    }
}
