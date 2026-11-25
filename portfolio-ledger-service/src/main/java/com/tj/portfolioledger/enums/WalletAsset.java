package com.tj.portfolioledger.enums;

public enum WalletAsset {
    BTC(8),
    ETH(8),
    USDT(8);

    private final int decimalPlaces;

    WalletAsset(int decimalPlaces) {
        this.decimalPlaces = decimalPlaces;
    }

    public int decimalPlaces() {
        return decimalPlaces;
    }

    public static WalletAsset fromCode(String currency) {
        if (currency == null) {
            throw new IllegalArgumentException("Wallet currency is required");
        }

        try {
            return valueOf(currency);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unsupported wallet currency", exception);
        }
    }
}
