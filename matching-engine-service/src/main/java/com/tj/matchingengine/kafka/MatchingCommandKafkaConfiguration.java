package com.tj.matchingengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class MatchingCommandKafkaConfiguration {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> matchingCommandContainerFactory(ConsumerFactory<?, ?> factory) {
        var properties = new HashMap<>(factory.getConfigurationProperties());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "matching-orders-v1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        var result = new ConcurrentKafkaListenerContainerFactory<String, String>();
        result.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), new StringDeserializer()));
        result.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        var errors = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.setClassifications(Map.of(), true);
        errors.setAckAfterHandle(false);
        errors.setCommitRecovered(false);
        result.setCommonErrorHandler(errors);

        return result;
    }
}
