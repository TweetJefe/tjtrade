package com.tj.orderrisk.config;
import com.tj.common.grpc.PortfolioServiceGrpc;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.ImportGrpcClients;
@Configuration(proxyBeanMethods = false)
@ImportGrpcClients(target = "portfolio-ledger-service", types = PortfolioServiceGrpc.PortfolioServiceBlockingStub.class)
public class PortfolioServiceGrpcConfig {}
