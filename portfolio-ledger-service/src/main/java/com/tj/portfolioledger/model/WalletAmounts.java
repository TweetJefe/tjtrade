package com.tj.portfolioledger.model;

import com.tj.portfolioledger.enums.WalletAsset;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class WalletAmounts {

    private static final BigDecimal UPPER_BOUND = new BigDecimal("100000000000000000000");

    private WalletAmounts() {
    }

    public static BigDecimal requireBalance(WalletAsset asset, BigDecimal amount) {
        if (asset == null || amount == null) {
            throw new IllegalArgumentException("Wallet asset and amount are required");
        }

        if (amount.signum() < 0 || amount.compareTo(UPPER_BOUND) >= 0) {
            throw new IllegalArgumentException("Wallet amount is outside the supported range");
        }

        if (amount.stripTrailingZeros().scale() > asset.decimalPlaces()) {
            throw new IllegalArgumentException("Wallet amount exceeds the supported decimal precision");
        }

        return amount.setScale(asset.decimalPlaces(), RoundingMode.UNNECESSARY);
    }

    public static BigDecimal requirePositive(WalletAsset asset, BigDecimal amount) {
        BigDecimal validated = requireBalance(asset, amount);

        if (validated.signum() == 0) {
            throw new IllegalArgumentException("Wallet operation amount must be greater than zero");
        }

        return validated;
    }
}
