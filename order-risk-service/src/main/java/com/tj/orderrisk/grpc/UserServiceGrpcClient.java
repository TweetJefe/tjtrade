package com.tj.orderrisk.grpc;


import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import org.springframework.beans.factory.annotation.Value;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UserServiceGrpcClient {

    private final UserServiceGrpc.UserServiceBlockingStub userStub;
    private final Duration deadline;

    public UserServiceGrpcClient(UserServiceGrpc.UserServiceBlockingStub userStub,
            @Value("${tjtrade.grpc.deadline:2s}") Duration deadline) {
        if (deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("gRPC deadline must be positive");
        }
        this.userStub = userStub;
        this.deadline = deadline;
    }

    public boolean checkUserExists(UUID userId){
        CheckUserRequest request = CheckUserRequest.newBuilder()
                .setUserId(userId.toString())
                .build();

        CheckUserResponse response = userStub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS).checkUserExists(request);

        return response.getExists();
    }
}
