package com.tj.portfolioledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(excludeName = {
        "org.springframework.boot.grpc.server.autoconfigure.GrpcServerAutoConfiguration"
})
public class PortfolioLedgerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PortfolioLedgerServiceApplication.class, args);
    }

}
