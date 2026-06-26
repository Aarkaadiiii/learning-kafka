package minchakov.arkadii.firstmicroservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic exampleTopic() {
        return TopicBuilder.name("product-created-events-topic")
            .partitions(3)
            .replicas(3)
            .config("min.insync.replicas", "2")
            .build();
    }
}
