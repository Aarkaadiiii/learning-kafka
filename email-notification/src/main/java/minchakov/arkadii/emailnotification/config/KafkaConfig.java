package minchakov.arkadii.emailnotification.config;

import jakarta.persistence.EntityManagerFactory;
import minchakov.arkadii.common.config.KafkaConsumerConfigurationProperties;
import minchakov.arkadii.common.config.KafkaProducerConfigurationProperties;
import minchakov.arkadii.common.config.MockServiceClientConfiguration;
import minchakov.arkadii.common.event.NotificationSentEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties({
    KafkaConsumerConfigurationProperties.class,
    KafkaProducerConfigurationProperties.class
})
@EnableKafkaRetryTopic
@Import(MockServiceClientConfiguration.class)
public class KafkaConfig {

    private final KafkaConsumerConfigurationProperties productCreatedEventsProps;

    private final KafkaProducerConfigurationProperties notificationSentEventsProps;

    public KafkaConfig(
        KafkaConsumerConfigurationProperties productCreatedEventsProps,
        KafkaProducerConfigurationProperties notificationSentEventsProps
    ) {
        this.productCreatedEventsProps = productCreatedEventsProps;
        this.notificationSentEventsProps = notificationSentEventsProps;
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

    @Bean
    public NewTopic notificationSentEventsTopic() {
        return TopicBuilder.name(notificationSentEventsProps.topic().name())
                           .partitions(notificationSentEventsProps.topic().partitions())
                           .replicas(notificationSentEventsProps.topic().replicas())
                           .config(
                               TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                               String.valueOf(notificationSentEventsProps.topic().minInsyncReplicas())
                           )
                           .build();
    }

    @Bean
    public Map<String, Object> notificationSentEventsProducerConfig() {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, productCreatedEventsProps.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, notificationSentEventsProps.idempotence());
        config.put(ProducerConfig.ACKS_CONFIG, notificationSentEventsProps.acks());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, notificationSentEventsProps.transactionIdPrefix());
        return config;
    }

    @Bean
    public ProducerFactory<String, NotificationSentEvent> notificationSentEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(notificationSentEventsProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, NotificationSentEvent> notificationSentEventKafkaTemplate() {
        return new KafkaTemplate<>(notificationSentEventProducerFactory());
    }

    @Bean
    public KafkaTransactionManager<String, NotificationSentEvent> notificationSentEventKafkaTransactionManager() {
        return new KafkaTransactionManager<>(notificationSentEventProducerFactory());
    }

    @Bean
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
