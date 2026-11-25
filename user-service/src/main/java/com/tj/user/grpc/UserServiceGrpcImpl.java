package com.tj.user.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import com.tj.user.repository.UserRepository;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.service.GrpcService;

import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class UserServiceGrpcImpl extends UserServiceGrpc.UserServiceImplBase {

    private final UserRepository repository;

    @Override
    public void checkUserExists(CheckUserRequest request, StreamObserver<CheckUserResponse> observer) {
        try {
            UUID userId = UUID.fromString(request.getUserId());
            var user = repository.findById(userId);
            var response = CheckUserResponse.newBuilder()
                    .setExists(user.isPresent())
                    .setStatus(user.map(value -> value.getStatus()).orElse("NOT_FOUND"))
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (IllegalArgumentException exception) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid user ID").asRuntimeException());
        } catch (Exception exception) {
            log.error("User identity lookup failed", exception);
            observer.onError(Status.INTERNAL.withDescription("User identity lookup failed").asRuntimeException());
        }
    }
}
