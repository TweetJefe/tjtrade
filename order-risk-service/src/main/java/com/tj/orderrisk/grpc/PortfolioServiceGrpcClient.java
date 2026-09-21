package com.tj.orderrisk.grpc;

import com.tj.common.grpc.LockFundsRequest;
import com.tj.common.grpc.LockFundsResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class PortfolioServiceGrpcClient {
    @GrpcClient("portfolio-ledger-service-grpc")
    private PortfolioServiceGrpc.PortfolioServiceBlockingStub portfolioStub;

    public boolean lockFunds(UUID userId, String asset, BigDecimal amount, UUID orderId) {
        LockFundsRequest request = LockFundsRequest.newBuilder()
                .setUserId(userId.toString())
                .setAsset(asset)
                .setAmount(amount.toString())
                .setOrderId(orderId.toString())
                .build();

        LockFundsResponse response = portfolioStub.lockFunds(request);
        return response.getSuccess();
    }
}
