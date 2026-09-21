package com.tj.portfolioledger.grpc;

import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.LockFundsResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.portfolioledger.repository.WalletBalanceRepository;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class PortfolioGrpcServiceImpl extends PortfolioServiceGrpc.PortfolioServiceImplBase {

    private final WalletBalanceRepository balanceRepository;

    @Override
    public void lockFunds(LockFundsRequest request, StreamObserver<LockFundsResponse> responseObserver) {
        log.info("Received gRPC request to lock funds: {} {} for user {}", request.getAmount(), request.getAsset(), request.getUserId());
        
        try {
            BigDecimal amountToLock = new BigDecimal(request.getAmount());
            UUID userId = UUID.fromString(request.getUserId());
            String asset = request.getAsset();
            
            // test
            balanceRepository.initCheatBalanceIfNeeded(userId, asset);

            Optional<BigDecimal> remaining = balanceRepository.lockFunds(userId, asset, amountToLock);

            LockFundsResponse response;
            if (remaining.isPresent()) {
                response = LockFundsResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Funds locked successfully. Remaining balance: " + remaining.get())
                        .build();
                log.info("Successfully locked {} {}. Remaining: {}", amountToLock, asset, remaining.get());
            } else {
                response = LockFundsResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Insufficient funds or wallet not found.")
                        .build();
                log.warn("Failed to lock funds for user {}. Requested: {}", userId, amountToLock);
            }


            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error processing lockFunds: ", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal Server Error: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
