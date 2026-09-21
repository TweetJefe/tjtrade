package com.tj.user.controller;

import com.google.api.Http;
import com.tj.common.dto.AccountDTO;
import com.tj.user.dto.CreateAccountRequest;
import com.tj.user.dto.DepositRequest;
import com.tj.user.dto.WithdrawalRequest;
import com.tj.user.service.AccountService;
import com.tj.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {
    private final UserService userService;
    private final AccountService accountService;

    @PostMapping("")
    public ResponseEntity<AccountDTO> createAccount(@RequestBody CreateAccountRequest request){
        AccountDTO newAccount = accountService.createAccount(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(newAccount);
    }

    @GetMapping("/user/{userId}/balance") //for market
    public ResponseEntity<BigDecimal> getBalanceByIdAndCurrency(
            @PathVariable UUID userId,
            @RequestParam String currency
            ){
        BigDecimal balance = accountService.getBalanceByCurrency(userId, currency);
        return ResponseEntity.ok(balance);
    }

    @GetMapping("/{accountId}/balance")//for user
    public ResponseEntity<BigDecimal> getBalanceByAccountId(
            @PathVariable UUID accountId
    ){
        BigDecimal balance = accountService.getBalanceByAccountId(accountId);
        return ResponseEntity.ok(balance);
    }

    @PatchMapping("/{accountId}/deposit")
    public ResponseEntity<AccountDTO> depositFunds(
            @PathVariable UUID accountId,
            @RequestBody DepositRequest request
    ){
        AccountDTO updatedAccount = accountService.depositFunds(accountId, request.amount());
        return ResponseEntity.status(HttpStatus.OK).body(updatedAccount);
    }

    @PatchMapping("/{accountId}/withdraw")
    public ResponseEntity<AccountDTO> withdrawFunds(
            @PathVariable UUID accountId,
            @RequestBody WithdrawalRequest request
    ){
        AccountDTO updatedAccount = accountService.withdrawFunds(accountId, request);
        return ResponseEntity.status(HttpStatus.OK).body(updatedAccount);
    }
}
