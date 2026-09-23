package com.tj.orderrisk.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.dto.RiskCheckResponse;
import com.tj.orderrisk.grpc.PortfolioServiceGrpcClient;
import com.tj.orderrisk.grpc.UserServiceGrpcClient;
import com.tj.orderrisk.kafka.OrderRiskKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class OrderRiskServiceImpl implements OrderRiskService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final DefaultRedisScript<Long> rateLimitScript;
    private final DefaultRedisScript<Long> reserveScript;
    private final OrderRiskKafkaProducer orderRiskKafkaProducer;
    private final UserServiceGrpcClient userClient;
    private final PortfolioServiceGrpcClient portfolioClient;

    public OrderRiskServiceImpl(
            RedisTemplate<String,
            Object> redisTemplate,
            OrderRiskKafkaProducer orderRiskKafkaProducer,
            UserServiceGrpcClient userClient,
            PortfolioServiceGrpcClient portfolioClient) {
        this.redisTemplate = redisTemplate;
        this.reserveScript = new DefaultRedisScript<>();
        this.orderRiskKafkaProducer = orderRiskKafkaProducer;
        this.reserveScript.setLocation(new ClassPathResource("luascripts/reserve_funds.lua"));
        this.reserveScript.setResultType(Long.class);
        this.userClient = userClient;
        this.portfolioClient = portfolioClient;
        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setLocation(new ClassPathResource("luascripts/rate_limit.lua"));
        this.rateLimitScript.setResultType(Long.class);
    }

    @Override
    public RiskCheckResponse validateOrderRisk(RiskCheckRequest request) {

        String rateLimitKey = "rate_limit:user:" + request.getUserId();

        Long allowed = redisTemplate.execute(rateLimitScript, List.of(rateLimitKey), "5", "1");

        if (allowed != null && allowed != 0L){
            log.warn("Rate limit exceeded for user {}", request.getUserId());
            return RiskCheckResponse.builder()
                    .approved(false)
                    .rejectReason("Rate limit exceeded. Maximum 5 orders per second.")
                    .build();
        }



//        boolean userExists = grpcClient.checkUserExists(request.getUserId());
//
//        if (!userExists){
//            log.warn("User {} doesn't exist", request.getUserId());
//            return RiskCheckResponse.builder()
//                    .rejectReason("User doesn't exists")
//                    .approved(false)
//                    .build();
//        }

        String[] currencies = request.getSymbol().split("_");
        String baseCurrency = currencies[0];
        String quoteCurrency = currencies[1];

        String currencyToLock;
        BigDecimal amountToLock;

        if (request.getSide() == OrderSide.BUY){
            currencyToLock = quoteCurrency;

            if(request.getType() == OrderType.MARKET){
                if (request.getPrice() == null || request.getPrice().compareTo(BigDecimal.ZERO) <= 0){
                    log.warn("MARKET BUY order rejected: missing budget/protection price");
                    return RiskCheckResponse.builder().approved(false)
                            .rejectReason("For MARKET BUY, you must specify a maximum protection price")
                            .build();
                }
            }
            amountToLock = request.getPrice().multiply(request.getQuantity());
        }else{
            currencyToLock = baseCurrency;
            amountToLock = request.getQuantity();
        }

        UUID orderId = UUID.randomUUID();


        log.info("Requesting ledger to lock {} {}", amountToLock, currencyToLock);

        boolean hasFunds = portfolioClient.lockFunds(
                request.getUserId(),
                currencyToLock,
                amountToLock,
                orderId
        );

        if(hasFunds){
            orderRiskKafkaProducer.sendOrderPlacedEvent(orderId, request);

            return RiskCheckResponse.builder()
                    .approved(true)
                    .build();
        }else{
            return RiskCheckResponse.builder()
                    .approved(false)
                    .rejectReason("Insufficient funds for " + currencyToLock)
                    .build();
        }
    }
}
