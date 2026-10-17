package com.tj.orderrisk.service;

import com.tj.common.enums.OrderType;
import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.dto.RiskCheckResponse;
import com.tj.orderrisk.grpc.PortfolioServiceGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderRiskServiceImpl implements OrderRiskService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final DefaultRedisScript<Long> rateLimitScript;
    private final PortfolioServiceGrpcClient portfolioClient;

    public OrderRiskServiceImpl(RedisTemplate<String, Object> redisTemplate, PortfolioServiceGrpcClient portfolioClient) {
        this.redisTemplate = redisTemplate;
        this.portfolioClient = portfolioClient;
        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setLocation(new ClassPathResource("luascripts/rate-limiter.lua"));
        this.rateLimitScript.setResultType(Long.class);
    }

    @Override
    public RiskCheckResponse validateOrderRisk(RiskCheckRequest request) {
        if (request == null || request.getOrderId() == null || request.getUserId() == null
                || request.getSide() == null || request.getSymbol() == null || request.getPrice() == null
                || request.getQuantity() == null || request.getPrice().signum() <= 0 || request.getQuantity().signum() <= 0) {
            throw new IllegalArgumentException("A valid order command with a stable orderId is required");
        }

        if (request.getType() != OrderType.LIMIT) {
            return rejected(request, "REJECTED", "Only LIMIT orders are supported");
        }

        Long allowed = redisTemplate.execute(rateLimitScript,
                List.of("rate_limit:user:" + request.getUserId()), "5", "1");

        if (allowed == null || (allowed != 0L && allowed != 1L)) {
            throw new IllegalStateException("Rate limiter returned an unexpected result");
        }

        if (allowed == 0L) {
            return rejected(request, "RATE_LIMITED", "Rate limit exceeded. Retry with the same orderId.");
        }

        try {
            var result = portfolioClient.acceptOrder(request);

            return RiskCheckResponse.builder()
                    .orderId(request.getOrderId())
                    .outcome(result.getOutcome().name())
                    .approved(true)
                    .build();
        } catch (StatusRuntimeException exception) {
            if (exception.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED) {
                return rejected(request, "REJECTED", "Insufficient funds or wallet not found");
            }

            // A transport failure is not proof of rejection: SQL may already be committed.
            throw exception;
        }
    }

    private RiskCheckResponse rejected(RiskCheckRequest request, String outcome, String reason) {
        return RiskCheckResponse.builder()
                .orderId(request.getOrderId())
                .outcome(outcome)
                .approved(false)
                .rejectReason(reason)
                .build();
    }
}
