package minchakov.arkadii.producteventsproducer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.kafka.producer")
public record KafkaProducerConfigurationProperties(
    String bootstrapServers,
    String keySerializer,
    String valueSerializer,
    String acks,
    String lingerMs,
    String idempotence,
    String maxInFlightRequestsPerConnection
) {
}
