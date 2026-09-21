package com.tj.orderrisk.grpc;


import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.common.grpc.UserServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UserServiceGrpcClient {

    @GrpcClient("user-service-grpc")
    private UserServiceGrpc.UserServiceBlockingStub userStub;

    public boolean checkUserExists(UUID userId){
        CheckUserRequest request = CheckUserRequest.newBuilder()
                .setUserId(userId.toString())
                .build();

        CheckUserResponse response = userStub.checkUserExists(request);

        return response.getExists();
    }
}
