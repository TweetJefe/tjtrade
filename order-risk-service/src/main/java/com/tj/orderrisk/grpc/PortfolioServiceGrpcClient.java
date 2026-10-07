package com.tj.orderrisk.grpc;

import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.LockFundsResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import org.springframework.beans.factory.annotation.Value;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class PortfolioServiceGrpcClient {
    private final PortfolioServiceGrpc.PortfolioServiceBlockingStub portfolioStub;
    private final Duration deadline;

    public PortfolioServiceGrpcClient(PortfolioServiceGrpc.PortfolioServiceBlockingStub portfolioStub,
            @Value("${tjtrade.grpc.deadline:2s}") Duration deadline) {
        if (deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("gRPC deadline must be positive");
        }
        this.portfolioStub = portfolioStub;
        this.deadline = deadline;
    }

    public boolean lockFunds(UUID userId, String asset, BigDecimal amount, UUID orderId) {
        LockFundsRequest request = LockFundsRequest.newBuilder()
                .setUserId(userId.toString())
                .setAsset(asset)
                .setAmount(amount.toString())
                .setOrderId(orderId.toString())
                .build();

        LockFundsResponse response = portfolioStub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS).lockFunds(request);
        return response.getSuccess();
    }
}
