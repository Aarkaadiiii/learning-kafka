package minchakov.arkadii.emailnotification.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaConsumerConfigurationProperties.class)
public class KafkaConfig {

    private final KafkaConsumerConfigurationProperties productCreatedEventsProps;

    public KafkaConfig(KafkaConsumerConfigurationProperties productCreatedEventsProps) {
        this.productCreatedEventsProps = productCreatedEventsProps;
    }

    @Bean
    public Map<String, Object> productCreatedEventsConsumerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, productCreatedEventsProps.bootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, productCreatedEventsProps.groupId());
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, productCreatedEventsProps.trustedPackages());
        return config;
    }

    @Bean
    public ConsumerFactory<String, Object> productCreatedEventsConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(productCreatedEventsConsumerConfig());
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, Object> productCreatedEventsContainerFactory() {
        var containerFactory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        containerFactory.setConsumerFactory(productCreatedEventsConsumerFactory());
        return containerFactory;
    }
}
