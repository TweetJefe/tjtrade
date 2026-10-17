package com.tj.portfolioledger.grpc;

import com.tj.common.grpc.AcceptOrderRequest;
import com.tj.common.grpc.AcceptOrderResponse;
import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.portfolioledger.command.AcceptOrderCommand;
import com.tj.portfolioledger.exception.AcceptOrderException;
import com.tj.portfolioledger.service.AcceptOrderService;
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
    private final AcceptOrderService acceptOrderService;

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
    @Override
    public void acceptOrder(AcceptOrderRequest request, StreamObserver<AcceptOrderResponse> observer) {
        try {
            OrderSide side = switch (request.getSide()) {
                case BUY -> OrderSide.BUY;
                case SELL -> OrderSide.SELL;
                default -> throw new IllegalArgumentException("Order side is required");
            };
            OrderType type = switch (request.getType()) {
                case LIMIT -> OrderType.LIMIT;
                default -> throw new IllegalArgumentException("Only LIMIT orders are supported");
            };
            var command = new AcceptOrderCommand(
                    UUID.fromString(request.getOrderId()), UUID.fromString(request.getUserId()),
                    request.getSymbol(), side, type, new BigDecimal(request.getPrice()), new BigDecimal(request.getQuantity())
            );
            var result = acceptOrderService.acceptOrder(command);
            var outcome = switch (result.outcome()) {
                case ACCEPTED -> AcceptOrderResponse.Outcome.ACCEPTED;
                case ALREADY_ACCEPTED -> AcceptOrderResponse.Outcome.ALREADY_ACCEPTED;
            };
            observer.onNext(AcceptOrderResponse.newBuilder()
                    .setOrderId(result.orderId().toString())
                    .setOutcome(outcome)
                    .build());
            observer.onCompleted();
        } catch (IllegalArgumentException exception) {
            observer.onError(io.grpc.Status.INVALID_ARGUMENT.withDescription("Invalid order command").asRuntimeException());
        } catch (AcceptOrderException exception) {
            var status = switch (exception.getReason()) {
                case ORDER_CONFLICT -> io.grpc.Status.ALREADY_EXISTS;
                case UNSUPPORTED_ORDER_TYPE, UNSUPPORTED_SYMBOL -> io.grpc.Status.INVALID_ARGUMENT;
                case INCONSISTENT_RESERVATION, INCONSISTENT_OUTBOX -> io.grpc.Status.DATA_LOSS;
            };
            observer.onError(status.withDescription("Order admission rejected").asRuntimeException());
        } catch (ReserveFundsException exception) {
            var status = switch (exception.getReason()) {
                case INSUFFICIENT_FUNDS_OR_WALLET_MISSING -> io.grpc.Status.RESOURCE_EXHAUSTED;
                case CONFLICT -> io.grpc.Status.ALREADY_EXISTS;
                case RESERVATION_CLOSED -> io.grpc.Status.FAILED_PRECONDITION;
            };
            observer.onError(status.withDescription("Order reservation rejected").asRuntimeException());
        } catch (Exception exception) {
            log.error("Order admission failed for order {}", request.getOrderId(), exception);
            observer.onError(io.grpc.Status.INTERNAL.withDescription("Order admission failed").asRuntimeException());
        }
    }
}
