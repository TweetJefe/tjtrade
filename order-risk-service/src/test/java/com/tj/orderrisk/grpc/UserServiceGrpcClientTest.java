package com.tj.orderrisk.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class UserServiceGrpcClientTest {
    @Test
    void slowRpcFailsWithConfiguredDeadline() throws Exception {
        String name = InProcessServerBuilder.generateName();
        var server = InProcessServerBuilder.forName(name).directExecutor()
                .addService(new UserServiceGrpc.UserServiceImplBase() {
                    @Override
                    public void checkUserExists(CheckUserRequest request, StreamObserver<CheckUserResponse> response) {
                        // Deliberately do not complete: the client must cancel without external infrastructure.
                    }
                }).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            var client = new UserServiceGrpcClient(UserServiceGrpc.newBlockingStub(channel), Duration.ofMillis(100));
            var failure = assertThrows(StatusRuntimeException.class, () -> client.checkUserExists(UUID.randomUUID()));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, failure.getStatus().getCode());
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test
    void rejectsNonPositiveDeadline() {
        assertThrows(IllegalArgumentException.class, () -> new UserServiceGrpcClient(null, Duration.ZERO));
    }
}
