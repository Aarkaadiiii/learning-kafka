package minchakov.arkadii.emailnotification.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.kafka.consumer")
public record KafkaConsumerConfigurationProperties(
    String bootstrapServers,
    String groupId,
    String trustedPackages
) {
}
