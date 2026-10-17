package com.tj.orderrisk.grpc;

import com.tj.common.grpc.AcceptOrderRequest;
import com.tj.common.grpc.AcceptOrderResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.orderrisk.dto.RiskCheckRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
public class PortfolioServiceGrpcClient {

    private final PortfolioServiceGrpc.PortfolioServiceBlockingStub portfolioStub;
    private final Duration deadline;

    public PortfolioServiceGrpcClient(
            PortfolioServiceGrpc.PortfolioServiceBlockingStub portfolioStub,
            @Value("${tjtrade.grpc.deadline:2s}") Duration deadline
    ) {
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("gRPC deadline must be positive");
        }

        this.portfolioStub = portfolioStub;
        this.deadline = deadline;
    }

    public AcceptOrderResponse acceptOrder(RiskCheckRequest command) {
        var request = AcceptOrderRequest.newBuilder()
                .setOrderId(command.getOrderId().toString())
                .setUserId(command.getUserId().toString())
                .setSymbol(command.getSymbol())
                .setSide(AcceptOrderRequest.Side.valueOf(command.getSide().name()))
                .setType(AcceptOrderRequest.Type.valueOf(command.getType().name()))
                .setPrice(command.getPrice().toPlainString())
                .setQuantity(command.getQuantity().toPlainString())
                .build();
        var response = portfolioStub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS).acceptOrder(request);

        if (!command.getOrderId().toString().equals(response.getOrderId())
                || (response.getOutcome() != AcceptOrderResponse.Outcome.ACCEPTED
                && response.getOutcome() != AcceptOrderResponse.Outcome.ALREADY_ACCEPTED)) {
            throw new IllegalStateException("Ledger returned an invalid admission response");
        }

        return response;
    }
}
