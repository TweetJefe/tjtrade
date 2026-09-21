package com.tj.user.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import com.tj.user.repository.UserRepository;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import java.util.UUID;

@GrpcService
@RequiredArgsConstructor
public class UserServiceGrpcImpl extends UserServiceGrpc.UserServiceImplBase{
    private final UserRepository repository;

    @Override
    public void checkUserExists (CheckUserRequest request, StreamObserver<CheckUserResponse> responseObserver){
        UUID userId = UUID.fromString(request.getUserId());

        boolean exists = repository.existsById(userId);

        CheckUserResponse response = CheckUserResponse.newBuilder()
                .setExists(exists)
                .setStatus(exists ? "ACTIVATED" : "NOT_FOUND")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
