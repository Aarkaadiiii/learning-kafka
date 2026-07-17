package minchakov.arkadii.producteventsproducer.config;

import minchakov.arkadii.common.config.KafkaProducerConfigurationProperties;
import minchakov.arkadii.common.event.ProductCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaProducerConfigurationProperties.class)
public class KafkaConfig {

    private final KafkaProducerConfigurationProperties productCreatedEventsProps;

    public KafkaConfig(KafkaProducerConfigurationProperties productCreatedEventsProps) {
        this.productCreatedEventsProps = productCreatedEventsProps;
    }

    @Bean
    public NewTopic productCreatedEventsTopic() {
        return TopicBuilder
            .name(productCreatedEventsProps.topic().name())
            .partitions(productCreatedEventsProps.topic().partitions())
            .replicas(productCreatedEventsProps.topic().replicas())
            .config(
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                String.valueOf(productCreatedEventsProps.topic().minInsyncReplicas())
            )
            .build();
    }

    @Bean
    public Map<String, Object> productCreatedEventsProducerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.ACKS_CONFIG, productCreatedEventsProps.acks());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, productCreatedEventsProps.bootstrapServers());
        config.put(ProducerConfig.LINGER_MS_CONFIG, productCreatedEventsProps.lingerMs());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, productCreatedEventsProps.idempotence());
        config.put(
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            productCreatedEventsProps.maxInFlightRequestsPerConnection()
        );
        return config;
    }

    @Bean
    public ProducerFactory<String, ProductCreatedEvent> productCreatedEventsProducerFactory() {
        return new DefaultKafkaProducerFactory<>(productCreatedEventsProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, ProductCreatedEvent> productCreatedEventsKafkaTemplate() {
        return new KafkaTemplate<>(productCreatedEventsProducerFactory());
    }
}
