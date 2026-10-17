package com.tj.orderrisk.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.common.grpc.AcceptOrderResponse;
import com.tj.orderrisk.dto.RiskCheckRequest;
import com.tj.orderrisk.grpc.PortfolioServiceGrpcClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
    private PortfolioServiceGrpcClient portfolioClient;
    private OrderRiskServiceImpl service;
    private RiskCheckRequest request;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        portfolioClient = mock(PortfolioServiceGrpcClient.class);
        service = new OrderRiskServiceImpl(redisTemplate, portfolioClient);
        request = RiskCheckRequest.builder()
                .orderId(UUID.randomUUID())
                .userId(UUID.randomUUID())
                .symbol("BTC_USDT")
                .side(OrderSide.BUY)
                .type(OrderType.LIMIT)
                .price(new BigDecimal("100"))
                .quantity(new BigDecimal("2"))
                .build();
    }

    @Test
    void allowedRequestUsesAtomicAdmissionAndPreservesOrderIdOnRetry() {
        rateLimitResult().thenReturn(1L);
        when(portfolioClient.acceptOrder(same(request))).thenReturn(accepted(AcceptOrderResponse.Outcome.ACCEPTED),
                accepted(AcceptOrderResponse.Outcome.ALREADY_ACCEPTED));

        var first = service.validateOrderRisk(request);
        var duplicate = service.validateOrderRisk(request);

        assertTrue(first.isApproved());
        assertEquals(request.getOrderId(), first.getOrderId());
        assertEquals("ACCEPTED", first.getOutcome());
        assertTrue(duplicate.isApproved());
        assertEquals(request.getOrderId(), duplicate.getOrderId());
        assertEquals("ALREADY_ACCEPTED", duplicate.getOutcome());
        verify(portfolioClient, times(2)).acceptOrder(same(request));
    }

    @Test
    void deniedRequestDoesNotCallLedger() {
        rateLimitResult().thenReturn(0L);

        var response = service.validateOrderRisk(request);

        assertFalse(response.isApproved());
        assertEquals("RATE_LIMITED", response.getOutcome());
        assertEquals(request.getOrderId(), response.getOrderId());
        verifyNoInteractions(portfolioClient);
    }

    @Test
    void fiveAllowedRequestsProceedAndSixthDeniedRequestStops() {
        rateLimitResult().thenReturn(1L, 1L, 1L, 1L, 1L, 0L);
        when(portfolioClient.acceptOrder(same(request))).thenReturn(accepted(AcceptOrderResponse.Outcome.ACCEPTED));

        for (int index = 0; index < 5; index++) {
            assertTrue(service.validateOrderRisk(request).isApproved());
        }

        assertFalse(service.validateOrderRisk(request).isApproved());
        verify(portfolioClient, times(5)).acceptOrder(same(request));
        verifyNoMoreInteractions(portfolioClient);
    }

    @Test
    void nullOrUnexpectedLimiterResultFailsBeforeAdmission() {
        rateLimitResult().thenReturn(null, 2L);

        assertThrows(IllegalStateException.class, () -> service.validateOrderRisk(request));
        assertThrows(IllegalStateException.class, () -> service.validateOrderRisk(request));
        verifyNoInteractions(portfolioClient);
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

    @Test
    void missingStableOrderIdAndMarketOrderCannotReserveFunds() {
        request.setOrderId(null);
        assertThrows(IllegalArgumentException.class, () -> service.validateOrderRisk(request));
        request.setOrderId(UUID.randomUUID());
        request.setType(OrderType.MARKET);

        assertFalse(service.validateOrderRisk(request).isApproved());
        verifyNoInteractions(portfolioClient, redisTemplate);
    }

    @Test
    void transportFailureRemainsUnknownRatherThanReportedAsRejection() {
        rateLimitResult().thenReturn(1L);
        when(portfolioClient.acceptOrder(same(request)))
                .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException());

        var failure = assertThrows(StatusRuntimeException.class, () -> service.validateOrderRisk(request));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, failure.getStatus().getCode());
    }

    @Test
    void insufficientFundsIsAConfirmedBusinessRejection() {
        rateLimitResult().thenReturn(1L);
        when(portfolioClient.acceptOrder(same(request)))
                .thenThrow(Status.RESOURCE_EXHAUSTED.asRuntimeException());

        var response = service.validateOrderRisk(request);

        assertFalse(response.isApproved());
        assertEquals("REJECTED", response.getOutcome());
        assertEquals(request.getOrderId(), response.getOrderId());
    }

    private AcceptOrderResponse accepted(AcceptOrderResponse.Outcome outcome) {
        return AcceptOrderResponse.newBuilder().setOrderId(request.getOrderId().toString()).setOutcome(outcome).build();
    }

    private org.mockito.stubbing.OngoingStubbing<Long> rateLimitResult() {
        return when(redisTemplate.execute(ArgumentMatchers.<RedisScript<Long>>any(),
                eq(List.of("rate_limit:user:" + request.getUserId())), eq("5"), eq("1")));
    }
}
