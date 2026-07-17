package minchakov.arkadii.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.kafka.producer")
public record KafkaProducerConfigurationProperties(
    KafkaTopicConfigurationProperties topic,
    String bootstrapServers,
    String acks,
    String lingerMs,
    String idempotence,
    String maxInFlightRequestsPerConnection,
    String transactionIdPrefix
) {
}
