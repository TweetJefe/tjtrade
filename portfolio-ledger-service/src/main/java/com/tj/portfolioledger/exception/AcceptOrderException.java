package com.tj.portfolioledger.exception;

import lombok.Getter;

@Getter
public class AcceptOrderException extends RuntimeException {

    public enum Reason {
        ORDER_CONFLICT,
        UNSUPPORTED_ORDER_TYPE,
        UNSUPPORTED_SYMBOL,
        INCONSISTENT_RESERVATION
    }

    private final Reason reason;

    public AcceptOrderException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }
}
