package com.tj.orderrisk.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.grpc.PortfolioServiceGrpcClient;
import com.tj.orderrisk.grpc.UserServiceGrpcClient;
import com.tj.orderrisk.kafka.OrderRiskKafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OrderRiskServiceImplTest {
    private RedisTemplate<String, Object> redisTemplate;
    private OrderRiskKafkaProducer producer;
    private PortfolioServiceGrpcClient portfolioClient;
    private OrderRiskServiceImpl service;
    private RiskCheckRequest request;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        producer = mock(OrderRiskKafkaProducer.class);
        portfolioClient = mock(PortfolioServiceGrpcClient.class);
        service = new OrderRiskServiceImpl(redisTemplate, producer,
                mock(UserServiceGrpcClient.class), portfolioClient);
        request = RiskCheckRequest.builder()
                .userId(UUID.randomUUID())
                .symbol("BTC_USDT")
                .side(OrderSide.BUY)
                .type(OrderType.LIMIT)
                .price(new BigDecimal("100"))
                .quantity(new BigDecimal("2"))
                .build();
    }

    @Test
    void allowedRequestLocksFundsAndPublishesOrder() {
        rateLimitResult().thenReturn(1L);
        when(portfolioClient.lockFunds(eq(request.getUserId()), eq("USDT"),
                eq(new BigDecimal("200")), any(UUID.class))).thenReturn(true);

        assertTrue(service.validateOrderRisk(request).isApproved());

        ArgumentCaptor<UUID> orderId = ArgumentCaptor.forClass(UUID.class);
        verify(portfolioClient).lockFunds(eq(request.getUserId()), eq("USDT"),
                eq(new BigDecimal("200")), orderId.capture());
        verify(producer).sendOrderPlacedEvent(eq(orderId.getValue()), same(request));
    }

    @Test
    void deniedRequestDoesNotLockFundsOrPublishOrder() {
        rateLimitResult().thenReturn(0L);

        var response = service.validateOrderRisk(request);

        assertFalse(response.isApproved());
        assertEquals("Rate limit exceeded. Maximum 5 orders per second.", response.getRejectReason());
        verifyNoInteractions(portfolioClient, producer);
    }

    @Test
    void fiveAllowedRequestsProceedAndSixthDeniedRequestStops() {
        rateLimitResult().thenReturn(1L, 1L, 1L, 1L, 1L, 0L);
        when(portfolioClient.lockFunds(any(UUID.class), eq("USDT"),
                eq(new BigDecimal("200")), any(UUID.class))).thenReturn(true);

        for (int i = 0; i < 5; i++) {
            assertTrue(service.validateOrderRisk(request).isApproved());
        }
        assertFalse(service.validateOrderRisk(request).isApproved());

        verify(portfolioClient, times(5)).lockFunds(eq(request.getUserId()), eq("USDT"),
                eq(new BigDecimal("200")), any(UUID.class));
        verify(producer, times(5)).sendOrderPlacedEvent(any(UUID.class), same(request));
        verifyNoMoreInteractions(portfolioClient, producer);
    }

    @Test
    void nullResultFailsBeforeFundsAreLocked() {
        rateLimitResult().thenReturn((Long) null);

        assertThrows(IllegalStateException.class, () -> service.validateOrderRisk(request));

        verifyNoInteractions(portfolioClient, producer);
    }

    @Test
    void unexpectedResultFailsBeforeFundsAreLocked() {
        rateLimitResult().thenReturn(2L);

        assertThrows(IllegalStateException.class, () -> service.validateOrderRisk(request));

        verifyNoInteractions(portfolioClient, producer);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void loadsExistingRateLimiterScriptAndUsesPerUserKey() throws Exception {
        rateLimitResult().thenReturn(0L);
        service.validateOrderRisk(request);
        ArgumentCaptor<RedisScript<Long>> script = (ArgumentCaptor) ArgumentCaptor.forClass(RedisScript.class);

        verify(redisTemplate).execute(script.capture(),
                eq(List.of("rate_limit:user:" + request.getUserId())), eq("5"), eq("1"));
        assertEquals(Long.class, script.getValue().getResultType());
        assertEquals(new ClassPathResource("luascripts/rate-limiter.lua").getContentAsString(
                java.nio.charset.StandardCharsets.UTF_8), script.getValue().getScriptAsString());
    }

    private org.mockito.stubbing.OngoingStubbing<Long> rateLimitResult() {
        return when(redisTemplate.execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("rate_limit:user:" + request.getUserId())), eq("5"), eq("1")));
    }
}
