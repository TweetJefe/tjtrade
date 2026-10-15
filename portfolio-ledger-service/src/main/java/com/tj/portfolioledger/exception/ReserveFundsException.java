package com.tj.portfolioledger.exception;

import lombok.Getter;

@Getter
public class ReserveFundsException extends RuntimeException {
  public enum Reason {
    CONFLICT,
    RESERVATION_CLOSED,
    INSUFFICIENT_FUNDS_OR_WALLET_MISSING
  }

  private final Reason reason;

  public ReserveFundsException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }
}
