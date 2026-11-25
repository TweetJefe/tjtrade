package com.tj.portfolioledger.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.UserServiceGrpc;
import com.tj.portfolioledger.exception.WalletException;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class WalletUserClient {

    private final UserServiceGrpc.UserServiceBlockingStub stub;
    private final Duration deadline;

    public WalletUserClient(UserServiceGrpc.UserServiceBlockingStub stub,
            @Value("${tjtrade.grpc.deadline:2s}") Duration deadline) {
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("Identity deadline must be positive");
        }

        this.stub = stub;
        this.deadline = deadline;
    }

    public void requireActive(UUID userId) {
        try {
            var response = stub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
                    .checkUserExists(CheckUserRequest.newBuilder().setUserId(userId.toString()).build());

            if (!response.getExists()) {
                throw new WalletException(WalletException.Reason.USER_NOT_FOUND);
            }

            if (!"ACTIVE".equals(response.getStatus())) {
                throw new WalletException(WalletException.Reason.USER_NOT_ACTIVE);
            }
        } catch (StatusRuntimeException exception) {
            throw new WalletException(WalletException.Reason.IDENTITY_UNAVAILABLE);
        }
    }
}
