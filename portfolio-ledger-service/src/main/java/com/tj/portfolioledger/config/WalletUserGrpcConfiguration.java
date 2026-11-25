package com.tj.portfolioledger.config;

import com.tj.common.grpc.UserServiceGrpc;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.ImportGrpcClients;

@Configuration(proxyBeanMethods = false)
@ImportGrpcClients(target = "user-service", types = UserServiceGrpc.UserServiceBlockingStub.class)
public class WalletUserGrpcConfiguration {
}
