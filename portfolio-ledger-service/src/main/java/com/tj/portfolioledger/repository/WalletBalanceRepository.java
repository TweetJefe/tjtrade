package com.tj.portfolioledger.repository;

import com.tj.portfolioledger.dto.response.WalletResponses;
import com.tj.portfolioledger.enums.WalletAsset;
import com.tj.portfolioledger.jooq.tables.records.WalletBalancesRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.tj.portfolioledger.jooq.Tables.WALLET_BALANCES;

@Repository
@RequiredArgsConstructor
public class WalletBalanceRepository {

    private final DSLContext dsl;

    public Optional<WalletResponses.WalletResponse> insertZeroWallet(UUID userId, WalletAsset asset) {
        return dsl.insertInto(WALLET_BALANCES)
                .set(WALLET_BALANCES.USER_ID, userId)
                .set(WALLET_BALANCES.CURRENCY, asset.name())
                .set(WALLET_BALANCES.AVAILABLE, BigDecimal.ZERO)
                .set(WALLET_BALANCES.LOCKED, BigDecimal.ZERO)
                .onConflict(WALLET_BALANCES.USER_ID, WALLET_BALANCES.CURRENCY)
                .doNothing()
                .returning()
                .fetchOptional(this::wallet);
    }

    public Optional<WalletResponses.WalletResponse> findWallet(UUID userId, WalletAsset asset) {
        return dsl.selectFrom(WALLET_BALANCES)
                .where(WALLET_BALANCES.USER_ID.eq(userId))
                .and(WALLET_BALANCES.CURRENCY.eq(asset.name()))
                .fetchOptional(this::wallet);
    }

    public Optional<WalletResponses.WalletResponse> findOwnedWallet(UUID userId, UUID walletId) {
        return dsl.selectFrom(WALLET_BALANCES)
                .where(WALLET_BALANCES.ID.eq(walletId))
                .and(WALLET_BALANCES.USER_ID.eq(userId))
                .fetchOptional(this::wallet);
    }

    public List<WalletResponses.WalletResponse> findWalletsByUser(UUID userId) {
        return dsl.selectFrom(WALLET_BALANCES)
                .where(WALLET_BALANCES.USER_ID.eq(userId))
                .orderBy(WALLET_BALANCES.CURRENCY.asc(), WALLET_BALANCES.ID.asc())
                .fetch(this::wallet);
    }

    private WalletResponses.WalletResponse wallet(WalletBalancesRecord record) {
        try {
            return new WalletResponses.WalletResponse(record.getId(), record.getUserId(), record.getCurrency(),
                    record.getAvailable(), record.getLocked());
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException("Stored wallet violates the wallet contract", exception);
        }
    }

    public Optional<BigDecimal> lockFunds(UUID userId, String asset, BigDecimal amountToLock) {
        var record = dsl.update(WALLET_BALANCES)
                .set(WALLET_BALANCES.AVAILABLE, WALLET_BALANCES.AVAILABLE.minus(amountToLock))
                .set(WALLET_BALANCES.LOCKED, WALLET_BALANCES.LOCKED.plus(amountToLock))
                .set(WALLET_BALANCES.UPDATED_AT, OffsetDateTime.now(ZoneOffset.UTC))
                .where(WALLET_BALANCES.USER_ID.eq(userId))
                .and(WALLET_BALANCES.CURRENCY.eq(asset))
                .and(WALLET_BALANCES.AVAILABLE.ge(amountToLock))
                .returningResult(WALLET_BALANCES.AVAILABLE)
                .fetchOne();

        if (record != null) {
            return Optional.of(record.value1());
        }
        return Optional.empty(); 
    }

    public void ensureWalletExists(UUID userId, String asset){
        dsl.insertInto(WALLET_BALANCES)
                .set(WALLET_BALANCES.USER_ID, userId)
                .set(WALLET_BALANCES.CURRENCY, asset)
                .set(WALLET_BALANCES.AVAILABLE, BigDecimal.ZERO)
                .set(WALLET_BALANCES.LOCKED, BigDecimal.ZERO)
                .onConflict(WALLET_BALANCES.USER_ID, WALLET_BALANCES.CURRENCY)
                .doNothing()
                .execute();
    }

    public void settleTrade(UUID buyerId, UUID sellerId, String baseAsset, String quoteAsset, BigDecimal baseAmount, BigDecimal quoteAmount){
        ensureWalletExists(buyerId, baseAsset);
        ensureWalletExists(sellerId, quoteAsset);

        dsl.update(WALLET_BALANCES)
                .set(WALLET_BALANCES.LOCKED, WALLET_BALANCES.LOCKED.minus(quoteAmount))
                .where(WALLET_BALANCES.USER_ID.eq(buyerId))
                .and(WALLET_BALANCES.CURRENCY.eq(quoteAsset))
                .execute();

        dsl.update(WALLET_BALANCES)
                .set(WALLET_BALANCES.AVAILABLE, WALLET_BALANCES.AVAILABLE.plus(baseAmount))
                .where(WALLET_BALANCES.USER_ID.eq(buyerId))
                .and(WALLET_BALANCES.CURRENCY.eq(baseAsset))
                .execute();

        dsl.update(WALLET_BALANCES)
                .set(WALLET_BALANCES.LOCKED, WALLET_BALANCES.LOCKED.minus(baseAmount))
                .where(WALLET_BALANCES.USER_ID.eq(sellerId))
                .and(WALLET_BALANCES.CURRENCY.eq(baseAsset))
                .execute();

        dsl.update(WALLET_BALANCES)
                .set(WALLET_BALANCES.AVAILABLE, WALLET_BALANCES.AVAILABLE.plus(quoteAmount))
                .where(WALLET_BALANCES.USER_ID.eq(sellerId))
                .and(WALLET_BALANCES.CURRENCY.eq(quoteAsset))
                .execute();
    }
}
