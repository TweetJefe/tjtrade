package com.tj.portfolioledger.messaging.outbox;

import com.tj.common.event.TradingMessages;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class OutboxPublisherConfiguration {

    @Bean
    FencedCommandProducerFactory commandProducerFactory(
            KafkaTemplate<String, Object> template,
            @Value("${tjtrade.kafka.namespace:tjtrade-local}") String namespace
    ) {
        return new FencedCommandProducerFactory(template.getProducerFactory().getConfigurationProperties(), namespace);
    }

    @Bean
    NewTopic matchingCommandsTopic(
            @Value("${tjtrade.kafka.command-partitions:3}") int partitions,
            @Value("${tjtrade.kafka.command-replicas:1}") int replicas
    ) {
        if (partitions <= 0 || replicas <= 0 || replicas > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid command topic topology");
        }

        return new NewTopic(TradingMessages.MATCHING_COMMANDS, partitions, (short) replicas)
                .configs(java.util.Map.of("min.insync.replicas", Integer.toString(Math.min(2, replicas))));
    }
}
