package com.tj.gateway.config;


import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Configuration
public class RateLimiterConfig {

    @Bean
    public KeyResolver ipKeyResolver(){
        return exchange -> Mono.just(
                exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()
        );
    }


    @Bean
    public KeyResolver userKeyResolver(){
        return exchange -> {
            String userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");
            if(userId != null && !userId.isEmpty()){
                return Mono.just(userId);
            }

            return Mono.just(
                    Objects.requireNonNull(exchange.getRequest().getRemoteAddress().getAddress().getHostName())
            );
        };
    }
}
