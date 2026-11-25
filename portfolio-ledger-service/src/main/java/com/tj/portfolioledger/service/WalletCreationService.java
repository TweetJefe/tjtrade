package com.tj.portfolioledger.service;

import com.tj.portfolioledger.enums.WalletAsset;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import com.tj.portfolioledger.result.WalletResults;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WalletCreationService {

    private final WalletBalanceRepository repository;

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public WalletResults.Creation create(UUID userId, WalletAsset asset) {
        var inserted = repository.insertZeroWallet(userId, asset);

        if (inserted.isPresent()) {
            return new WalletResults.Creation(inserted.get(), true);
        }

        var existing = repository.findWallet(userId, asset)
                .orElseThrow(() -> new IllegalStateException("Conflicting wallet was not found"));

        return new WalletResults.Creation(existing, false);
    }
}
