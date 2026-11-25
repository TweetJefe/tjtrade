package com.tj.portfolioledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.grpc.server.autoconfigure.security.GrpcServerOAuth2ResourceServerAutoConfiguration;

// Wallet HTTP JWT must not implicitly change the existing native gRPC TLS contract.
@SpringBootApplication(exclude = GrpcServerOAuth2ResourceServerAutoConfiguration.class)
public class PortfolioLedgerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PortfolioLedgerServiceApplication.class, args);
    }

}
