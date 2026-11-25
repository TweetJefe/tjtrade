package com.tj.portfolioledger.exception;

import lombok.Getter;

@Getter
public class WalletException extends RuntimeException {

    public enum Reason {
        WALLET_NOT_FOUND,
        USER_NOT_FOUND,
        USER_NOT_ACTIVE,
        IDENTITY_UNAVAILABLE
    }

    private final Reason reason;

    public WalletException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }
}
