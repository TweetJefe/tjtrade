package com.tj.orderrisk.controller;

import io.grpc.StatusRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OrderAdmissionExceptionHandler {

    @ExceptionHandler(StatusRuntimeException.class)
    public ResponseEntity<ProblemDetail> grpcFailure(StatusRuntimeException exception) {
        HttpStatus status = switch (exception.getStatus().getCode()) {
            case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
            case ALREADY_EXISTS, FAILED_PRECONDITION -> HttpStatus.CONFLICT;
            default -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        String detail = status == HttpStatus.SERVICE_UNAVAILABLE
                ? "Order acceptance is unknown. Retry the same command with the same orderId."
                : "Order command was rejected. A reused orderId must have identical parameters.";
        var problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle("Order admission failed");

        return ResponseEntity.status(status).body(problem);
    }

    @ExceptionHandler({IllegalArgumentException.class,
            org.springframework.web.bind.MethodArgumentNotValidException.class,
            org.springframework.http.converter.HttpMessageNotReadableException.class})
    public ResponseEntity<ProblemDetail> invalidCommand(Exception exception) {
        return ResponseEntity.badRequest().body(ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Invalid order command"
        ));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ProblemDetail> unavailable(IllegalStateException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE, "Order acceptance is unknown. Retry with the same orderId."
        ));
    }
}
