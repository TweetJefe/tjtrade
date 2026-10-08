package com.tj.portfolioledger.service;

import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.enums.ReserveFundsOutcome;
import com.tj.portfolioledger.exception.ReserveFundsException;
import com.tj.portfolioledger.model.FundReservation;
import com.tj.portfolioledger.repository.FundReservationRepository;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import com.tj.portfolioledger.result.ReserveFundsResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class ReserveFundsService {

    private final FundReservationRepository reservationRepository;
    private final WalletBalanceRepository walletBalanceRepository;

    private static final BigDecimal AMOUNT_UPPER_BOUND = new BigDecimal("100000000000000000000");



    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReserveFundsResult reserveFunds (ReserveFundsCommand command){
        validate(command);

        boolean inserted = reservationRepository.insertIfAbsent(command);

        if(!inserted){ return handleExistingReservation(command);}

        var remainingBalance = walletBalanceRepository.lockFunds(
                command.userId(),
                command.asset(),
                command.amount()
        );

        if(remainingBalance.isEmpty()){
            throw new ReserveFundsException(
                    ReserveFundsException
                            .Reason
                            .INSUFFICIENT_FUNDS_OR_WALLET_MISSING,
                    "Insufficient funds or wallet was not found");
        }

        return new ReserveFundsResult(
                command.orderId(),
                ReserveFundsOutcome.RESERVED
        );
    }

    private ReserveFundsResult handleExistingReservation(ReserveFundsCommand command){
        FundReservation existing = reservationRepository
                .findByOrderIdForUpdate(command.orderId())
                .orElseThrow(() -> new IllegalStateException(
                        "Conflicting reservation was not found"
                ));

        boolean sameParameters = existing.userId().equals(command.userId())
                && existing.asset().equals(command.asset())
                && existing.amount().compareTo(command.amount()) == 0;

        if(!sameParameters){
            throw new ReserveFundsException(
                    ReserveFundsException
                            .Reason
                            .CONFLICT,
                    "Order Id is already associated with another reservation"
            );
        }

        if(!"ACTIVE".equals(existing.status())){
            throw new ReserveFundsException(
                    ReserveFundsException
                            .Reason
                            .RESERVATION_CLOSED,
                    "Reservation is already closed"
            );
        }

        return new ReserveFundsResult(
                command.orderId(),
                ReserveFundsOutcome.ALREADY_RESERVED
        );
    }

    private void validate(ReserveFundsCommand command){
        if(command == null){
            throw new IllegalArgumentException("Command is required");
        }


        if(command.orderId() == null || command.userId() == null){
            throw new IllegalArgumentException(
                    "Order ID and user ID are required"
            );
        }

        String asset = command.asset();

        if(asset == null
            || asset.isBlank()
            || asset.length() > 20
            || !asset.equals(asset.strip())){
            throw new IllegalArgumentException("Invalid asset");
        }

        BigDecimal amount = command.amount();

        if (amount == null || amount.signum() <= 0){
            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }

        if (amount.stripTrailingZeros().scale() > 8) {
            throw new IllegalArgumentException(
                    "Amount must have at most 8 decimal places"
            );
        }

        if (amount.compareTo(AMOUNT_UPPER_BOUND) >= 0) {
            throw new IllegalArgumentException(
                    "Amount exceeds the supported range"
            );
        }
    }
}
