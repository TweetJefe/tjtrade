package com.tj.portfolioledger.service;

import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.repository.FundReservationRepository;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ReserveFundsServiceTest {

    @Test
    void invalidAssetAndPrecisionFailBeforeFinancialWrites() {
        var reservations = mock(FundReservationRepository.class);
        var wallets = mock(WalletBalanceRepository.class);
        var service = new ReserveFundsService(reservations, wallets);

        for (String asset : new String[]{"USD", "btc", " USDT"}) {
            assertThrows(IllegalArgumentException.class, () -> service.reserveFunds(
                    new ReserveFundsCommand(UUID.randomUUID(), UUID.randomUUID(), asset, BigDecimal.ONE)));
        }

        assertThrows(IllegalArgumentException.class, () -> service.reserveFunds(
                new ReserveFundsCommand(UUID.randomUUID(), UUID.randomUUID(), "BTC", new BigDecimal("0.000000001"))));
        verifyNoInteractions(reservations, wallets);
    }
}
