package com.tj.orderrisk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(excludeName = {
        "org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration",
        "org.springframework.boot.grpc.server.autoconfigure.GrpcServerAutoConfiguration"
})
public class OrderRiskServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderRiskServiceApplication.class, args);
    }

}
