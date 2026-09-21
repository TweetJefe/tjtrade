package com.tj.user.repository;

import com.tj.user.exception.AccountNotFoundException;
import com.tj.user.exception.InsufficientFundsException;
import com.tj.user.jooq.tables.records.AccountsRecord;
import com.tj.user.model.Account;
import io.lettuce.core.BitFieldArgs;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import javax.naming.InsufficientResourcesException;

import static com.tj.user.jooq.Tables.ACCOUNTS;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class AccountRepository {
    private final DSLContext dsl;

    public boolean existsById(UUID accountId){
        return dsl.fetchExists(
                dsl.selectFrom(ACCOUNTS)
                        .where(ACCOUNTS.ID.eq(accountId))
        );
    }

    public boolean existsByIdAndCurrency(UUID userId, String currency){
        return dsl.fetchExists(
                dsl.selectFrom(ACCOUNTS)
                        .where(ACCOUNTS.USER_ID.eq(userId))
                        .and(ACCOUNTS.CURRENCY.eq(currency))
        );
    }

    public Account save(UUID userId, String currency){
        var record = dsl.insertInto(ACCOUNTS)
                .set(ACCOUNTS.USER_ID, userId)
                .set(ACCOUNTS.CURRENCY, currency)
                .set(ACCOUNTS.BALANCE, BigDecimal.ZERO)
                .set(ACCOUNTS.RESERVED_BALANCE, BigDecimal.ZERO)
                .returning()
                .fetchOne();

        return record.into(Account.class);
    }

    public Optional<BigDecimal> getBalanceByCurrency(UUID userId, String currency){
        return dsl.select(ACCOUNTS.BALANCE)
                .from(ACCOUNTS)
                .where(ACCOUNTS.USER_ID.eq(userId))
                .and(ACCOUNTS.CURRENCY.eq(currency))
                .fetchOptionalInto(BigDecimal.class);
    }


    public Optional<BigDecimal> getBalanceByAccountId(UUID accountId){
        return dsl.select(ACCOUNTS.BALANCE)
                .from(ACCOUNTS)
                .where(ACCOUNTS.ID.eq(accountId))
                .fetchOptionalInto(BigDecimal.class);
    }

    public Optional<Account> depositFunds(UUID accountId, BigDecimal amount) {
        var record = dsl.update(ACCOUNTS)
                .set(ACCOUNTS.BALANCE, ACCOUNTS.BALANCE.add(amount))
                .set(ACCOUNTS.UPDATED_AT, OffsetDateTime.now())
                .where(ACCOUNTS.ID.eq(accountId))
                .returning()
                .fetchOne();
        return Optional.ofNullable(record != null ? record.into(Account.class) : null);
    }

    public Optional<Account> withdrawFunds(UUID accountId, BigDecimal amount) {
        AccountsRecord record = dsl.selectFrom(ACCOUNTS)
                .where(ACCOUNTS.ID.eq(accountId))
                .forUpdate()
                .fetchOne();

        if(record == null){
            throw new AccountNotFoundException("Account not found");
        }

        if(record.getBalance().compareTo(amount) < 0){
            throw new InsufficientFundsException("Not enough money");
        }

        record.setBalance(record.getBalance().subtract(amount));
        record.setUpdatedAt(OffsetDateTime.now());
        record.store();

        return Optional.of(record.into(Account.class));
    }

    public Optional<Account> unreserveFunds(UUID userId, String currency, BigDecimal amount) {
        var record = dsl.update(ACCOUNTS)
                .set(ACCOUNTS.RESERVED_BALANCE, ACCOUNTS.RESERVED_BALANCE.subtract(amount))
                .set(ACCOUNTS.BALANCE, ACCOUNTS.BALANCE.add(amount))
                .set(ACCOUNTS.UPDATED_AT, OffsetDateTime.now())
                .where(ACCOUNTS.USER_ID.eq(userId))
                .and(ACCOUNTS.CURRENCY.eq(currency))
                .and(ACCOUNTS.RESERVED_BALANCE.ge(amount))
                .returning()
                .fetchOne();
        return Optional.ofNullable(record != null ? record.into(Account.class) : null);
    }

    public Optional<Account> deductFunds(UUID userId, String currency, BigDecimal amount) {
        var record = dsl.update(ACCOUNTS)
                .set(ACCOUNTS.RESERVED_BALANCE, ACCOUNTS.RESERVED_BALANCE.subtract(amount))
                .set(ACCOUNTS.UPDATED_AT, OffsetDateTime.now())
                .where(ACCOUNTS.USER_ID.eq(userId))
                .and(ACCOUNTS.CURRENCY.eq(currency))
                .and(ACCOUNTS.RESERVED_BALANCE.ge(amount))
                .returning()
                .fetchOne();
        return Optional.ofNullable(record != null ? record.into(Account.class) : null);
    }

    public Optional<Account> depositFundsByUserId(UUID userId, String currency, BigDecimal amount) {
        var record = dsl.update(ACCOUNTS)
                .set(ACCOUNTS.BALANCE, ACCOUNTS.BALANCE.add(amount))
                .set(ACCOUNTS.UPDATED_AT, OffsetDateTime.now())
                .where(ACCOUNTS.USER_ID.eq(userId))
                .and(ACCOUNTS.CURRENCY.eq(currency))
                .returning()
                .fetchOne();
        return Optional.ofNullable(record != null ? record.into(Account.class) : null);
    }

    public Optional<Account> reserveFunds(UUID userId, String currency, BigDecimal amount) {
        var record = dsl.update(ACCOUNTS)
                .set(ACCOUNTS.BALANCE, ACCOUNTS.BALANCE.subtract(amount))
                .set(ACCOUNTS.RESERVED_BALANCE, ACCOUNTS.RESERVED_BALANCE.add(amount))
                .set(ACCOUNTS.UPDATED_AT, OffsetDateTime.now())
                .where(ACCOUNTS.USER_ID.eq(userId))
                .and(ACCOUNTS.CURRENCY.eq(currency))
                .and(ACCOUNTS.BALANCE.ge(amount))
                .returning()
                .fetchOne();
        return Optional.ofNullable(record != null ? record.into(Account.class) : null);
    }
}
