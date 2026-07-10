package minchakov.arkadii.emailnotification.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaConsumerConfigurationProperties.class)
@EnableKafkaRetryTopic
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
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
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

    @Bean
    public KafkaTemplate<String, Object> productCreatedEventsDLTKafkaTemplate() {
        return new KafkaTemplate<>(productCreatedEventsDLTProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> productCreatedEventsDLTProducerFactory() {
        return new DefaultKafkaProducerFactory<>(productCreatedEventsDLTProducerConfig());
    }

    @Bean
    public Map<String, Object> productCreatedEventsDLTProducerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, productCreatedEventsProps.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return config;
    }

    @Bean
    public TaskScheduler dltTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("kafka-dlt-");
        scheduler.initialize();
        return scheduler;
    }
}
