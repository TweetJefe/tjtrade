package com.tj.portfolioledger.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import com.tj.portfolioledger.exception.WalletException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WalletUserClientTest {

    @Test
    void activeUserIsAcceptedAndInactiveOrMissingUsersAreRejected() throws Exception {
        var userId = UUID.randomUUID();

        for (String status : new String[]{"ACTIVE", "CLOSED", "DELETED", "ACTIVATED", "NOT_FOUND"}) {
            String name = "wallet-" + UUID.randomUUID();
            var server = InProcessServerBuilder.forName(name).directExecutor()
                    .addService(new UserServiceGrpc.UserServiceImplBase() {
                        @Override
                        public void checkUserExists(CheckUserRequest request, StreamObserver<CheckUserResponse> observer) {
                            assertEquals(userId.toString(), request.getUserId());
                            observer.onNext(CheckUserResponse.newBuilder().setExists(!"NOT_FOUND".equals(status)).setStatus(status).build());
                            observer.onCompleted();
                        }
                    }).build().start();
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();

            try {
                var client = new WalletUserClient(UserServiceGrpc.newBlockingStub(channel), Duration.ofSeconds(1));

                if ("ACTIVE".equals(status)) {
                    assertDoesNotThrow(() -> client.requireActive(userId));
                } else {
                    var failure = assertThrows(WalletException.class, () -> client.requireActive(userId));
                    assertEquals("NOT_FOUND".equals(status) ? WalletException.Reason.USER_NOT_FOUND : WalletException.Reason.USER_NOT_ACTIVE, failure.getReason());
                }
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    @Test
    void slowIdentityCallFailsWithFiniteDeadline() throws Exception {
        String name = "slow-wallet-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).directExecutor().addService(new UserServiceGrpc.UserServiceImplBase() {
            @Override
            public void checkUserExists(CheckUserRequest request, StreamObserver<CheckUserResponse> observer) {
                // Keep the response pending to verify the real gRPC deadline.
            }
        }).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();

        try {
            var client = new WalletUserClient(UserServiceGrpc.newBlockingStub(channel), Duration.ofMillis(100));
            var failure = assertThrows(WalletException.class, () -> client.requireActive(UUID.randomUUID()));
            assertEquals(WalletException.Reason.IDENTITY_UNAVAILABLE, failure.getReason());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void nonpositiveDeadlineIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new WalletUserClient(null, Duration.ZERO));
    }
}
