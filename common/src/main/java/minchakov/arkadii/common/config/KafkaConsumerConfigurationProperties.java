package minchakov.arkadii.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.kafka.consumer")
public record KafkaConsumerConfigurationProperties(
    KafkaTopicConfigurationProperties topic,
    String bootstrapServers,
    String groupId,
    String trustedPackages
) {
}
