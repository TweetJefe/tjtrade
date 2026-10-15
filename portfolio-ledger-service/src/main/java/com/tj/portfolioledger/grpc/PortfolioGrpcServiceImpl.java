package com.tj.portfolioledger.grpc;

import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.LockFundsResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.exception.ReserveFundsException;
import com.tj.portfolioledger.result.ReserveFundsResult;
import com.tj.portfolioledger.service.ReserveFundsService;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.service.GrpcService;

import java.math.BigDecimal;

import java.util.UUID;


@Slf4j
@GrpcService
@RequiredArgsConstructor
public class PortfolioGrpcServiceImpl extends PortfolioServiceGrpc.PortfolioServiceImplBase {

    private final ReserveFundsService service;

    @Override
    public void lockFunds(LockFundsRequest request, StreamObserver<LockFundsResponse> responseObserver) {
        ReserveFundsResult result;

        try{
            ReserveFundsCommand command = new ReserveFundsCommand(
                    UUID.fromString(request.getOrderId()),
                    UUID.fromString(request.getUserId()),
                    request.getAsset(),
                    new BigDecimal(request.getAmount())
            );

            log.info(
                    "Received reservation request for order {}: {} {} for user {}",
                    command.orderId(),
                    command.amount(),
                    command.asset(),
                    command.userId());

            result = service.reserveFunds(command);
        }catch (IllegalArgumentException exception){
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Invalid reservation request")
                    .asRuntimeException());
            return;
        }catch (ReserveFundsException exception){
            switch (exception.getReason()) {
                case INSUFFICIENT_FUNDS_OR_WALLET_MISSING -> {
                    responseObserver.onNext(
                            LockFundsResponse.newBuilder()
                                    .setSuccess(false)
                                    .setMessage(
                                            "Insufficient funds or wallet not found"
                                    )
                                    .build()
                    );

                    responseObserver.onCompleted();
                }

                case CONFLICT -> responseObserver.onError(
                        io.grpc.Status.ALREADY_EXISTS
                                .withDescription(
                                        "Order ID has conflicting reservation parameters"
                                )
                                .asRuntimeException()
                );

                case RESERVATION_CLOSED -> responseObserver.onError(
                        io.grpc.Status.FAILED_PRECONDITION
                                .withDescription("Reservation is already closed")
                                .asRuntimeException()
                );
            }

            return;
        } catch (Exception exception) {
            log.error("Failed to reserve funds", exception);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Failed to reserve funds")
                    .asRuntimeException());
            return;
        }

        String message = switch(result.outcome()){
            case RESERVED -> "Funds reserved successfully";
            case ALREADY_RESERVED -> "Funds were already reserved";
        };

        LockFundsResponse response = LockFundsResponse.newBuilder()
                .setSuccess(true)
                .setMessage(message)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
