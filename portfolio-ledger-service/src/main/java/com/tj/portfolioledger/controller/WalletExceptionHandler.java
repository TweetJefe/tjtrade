package com.tj.portfolioledger.controller;

import com.tj.portfolioledger.exception.WalletException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@RestControllerAdvice(assignableTypes = WalletController.class)
public class WalletExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(WalletException.class)
    ResponseEntity<ProblemDetail> walletFailure(WalletException exception) {
        return switch (exception.getReason()) {
            case WALLET_NOT_FOUND -> problem(HttpStatus.NOT_FOUND, "Owned wallet was not found");
            case USER_NOT_FOUND -> problem(HttpStatus.NOT_FOUND, "User was not found");
            case USER_NOT_ACTIVE -> problem(HttpStatus.FORBIDDEN, "User is not active");
            case IDENTITY_UNAVAILABLE -> problem(HttpStatus.SERVICE_UNAVAILABLE, "Identity service is unavailable; retry later");
        };
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> invalidRequest(IllegalArgumentException exception) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid wallet request");
    }

    @ExceptionHandler({DataIntegrityViolationException.class, org.jooq.exception.IntegrityConstraintViolationException.class})
    ResponseEntity<ProblemDetail> conflict(RuntimeException exception) {
        log.warn("Wallet operation conflicted with database state", exception);
        return problem(HttpStatus.CONFLICT, "Wallet operation conflicts with current database state");
    }

    @ExceptionHandler({DataAccessException.class, org.jooq.exception.DataAccessException.class})
    ResponseEntity<ProblemDetail> unavailable(RuntimeException exception) {
        log.error("Wallet database operation failed", exception);
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Wallet storage is unavailable; retry later");
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> unexpected(Exception exception) {
        log.error("Wallet request failed", exception);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Wallet request failed");
    }

    private ResponseEntity<ProblemDetail> problem(HttpStatus status, String detail) {
        var body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setTitle(status.getReasonPhrase());

        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(body);
    }
}
