package com.tj.orderrisk.grpc;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.common.grpc.AcceptOrderRequest;
import com.tj.common.grpc.AcceptOrderResponse;
import com.tj.common.grpc.PortfolioServiceGrpc;
import com.tj.orderrisk.dto.RiskCheckRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PortfolioServiceGrpcClientTest {

    @Test
    void mapsCommandAndPreservesStableOrderId() throws Exception {
        var observed = new AtomicReference<AcceptOrderRequest>();
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new PortfolioServiceGrpc.PortfolioServiceImplBase() {
                    @Override
                    public void acceptOrder(AcceptOrderRequest request, StreamObserver<AcceptOrderResponse> observer) {
                        observed.set(request);
                        observer.onNext(AcceptOrderResponse.newBuilder().setOrderId(request.getOrderId())
                                .setOutcome(AcceptOrderResponse.Outcome.ALREADY_ACCEPTED).build());
                        observer.onCompleted();
                    }
                }).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        try {
            var request = request();
            var client = new PortfolioServiceGrpcClient(PortfolioServiceGrpc.newBlockingStub(channel), Duration.ofSeconds(1));
            assertEquals(AcceptOrderResponse.Outcome.ALREADY_ACCEPTED, client.acceptOrder(request).getOutcome());
            assertEquals(request.getOrderId().toString(), observed.get().getOrderId());
            assertEquals("100.00", observed.get().getPrice());
            assertEquals(AcceptOrderRequest.Side.BUY, observed.get().getSide());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void lostReplyUsesFiniteDeadline() throws Exception {
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new PortfolioServiceGrpc.PortfolioServiceImplBase() {
                    @Override
                    public void acceptOrder(AcceptOrderRequest request, StreamObserver<AcceptOrderResponse> observer) {
                        // An admission may have committed even though the response never arrives.
                    }
                }).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        try {
            var client = new PortfolioServiceGrpcClient(PortfolioServiceGrpc.newBlockingStub(channel), Duration.ofMillis(100));
            var failure = assertThrows(StatusRuntimeException.class, () -> client.acceptOrder(request()));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, failure.getStatus().getCode());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void rejectsInvalidDeadline() {
        assertThrows(IllegalArgumentException.class, () -> new PortfolioServiceGrpcClient(null, Duration.ZERO));
    }

    private RiskCheckRequest request() {
        return RiskCheckRequest.builder().orderId(UUID.randomUUID()).userId(UUID.randomUUID()).symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT).price(new BigDecimal("100.00"))
                .quantity(new BigDecimal("2")).build();
    }
}
