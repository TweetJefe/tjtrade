package com.tj.portfolioledger.service;

import com.tj.portfolioledger.dto.response.WalletResponses;
import com.tj.portfolioledger.enums.WalletAsset;
import com.tj.portfolioledger.exception.WalletException;
import com.tj.portfolioledger.grpc.WalletUserClient;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import com.tj.portfolioledger.result.WalletResults;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WalletService {

    private final WalletBalanceRepository repository;
    private final WalletCreationService creationService;
    private final WalletUserClient users;

    public WalletResults.Creation create(UUID userId, String currency) {
        Objects.requireNonNull(userId, "Authenticated user ID is required");
        WalletAsset asset = WalletAsset.fromCode(currency);
        users.requireActive(userId);

        return creationService.create(userId, asset);
    }

    @Transactional(readOnly = true)
    public WalletResponses.WalletResponse get(UUID userId, UUID walletId) {
        return repository.findOwnedWallet(userId, walletId)
                .orElseThrow(() -> new WalletException(WalletException.Reason.WALLET_NOT_FOUND));
    }

    @Transactional(readOnly = true)
    public List<WalletResponses.WalletResponse> list(UUID userId) {
        return repository.findWalletsByUser(userId);
    }
}
