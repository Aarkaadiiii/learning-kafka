package minchakov.arkadii.producteventsproducer.service;

import minchakov.arkadii.common.config.KafkaConsumerConfigurationProperties;
import minchakov.arkadii.common.event.ProductCreatedEvent;
import minchakov.arkadii.producteventsproducer.dto.ProductCreatedDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@EnableConfigurationProperties(KafkaConsumerConfigurationProperties.class)
@EmbeddedKafka(partitions = 3, topics = "${spring.kafka.consumer.topic.name}")
public class ProductsServiceIntegrationTests {

    @Autowired
    private ProductsService service;

    @Autowired
    private KafkaConsumerConfigurationProperties props;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String consumerAutoOffsetReset;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private ConsumerFactory<String, Object> consumerFactory;

    private ContainerProperties containerProperties;

    private ConcurrentLinkedQueue<ConsumerRecord<String, Object>> events;

    private KafkaMessageListenerContainer<String, Object> container;

    private boolean isInitialized = false;

    @BeforeEach
    void setUp() {
        if (!isInitialized) {
            var config = new HashMap<String, Object>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
            config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, props.trustedPackages());
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset);

            consumerFactory = new DefaultKafkaConsumerFactory<>(config);

            containerProperties = new ContainerProperties(props.topic().name());
            events = new ConcurrentLinkedQueue<>();
            containerProperties.setMessageListener((MessageListener<String, Object>) events::add);

            isInitialized = true;
        }

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
    }

    @AfterEach
    void tearDown() {
        container.stop();
        events.clear();
    }

    @Test
    @DisplayName("Event is produced to product-created-events-topic when valid dto is passed to ProductService")
    void createProduct_whenEverythingIsFine_shouldProduceEvent() {
        // Arrange
        var dto = new ProductCreatedDto(
            "Samsung Galaxy Z Fold5",
            BigDecimal.valueOf(100.0),
            1
        );

        // Act
        service.createProduct(dto);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !events.isEmpty());
        var event = events.poll();

        // Assert
        assertThat(event).isNotNull();
        assertThat(event.value()).isInstanceOf(ProductCreatedEvent.class);

        var productCreatedEvent = (ProductCreatedEvent) event.value();
        assertThat(productCreatedEvent).isNotNull();
        assertThat(productCreatedEvent.id()).isEqualTo(event.key());
        assertThat(productCreatedEvent.title()).isEqualTo(dto.title());
        assertThat(productCreatedEvent.price()).isEqualTo(dto.price());
        assertThat(productCreatedEvent.quantity()).isEqualTo(dto.quantity());

        var messageIdString = new String(event.headers().lastHeader("messageId").value(), StandardCharsets.UTF_8);
        var messageId = UUID.fromString(messageIdString);
        var productId = UUID.fromString(productCreatedEvent.id());
        assertThat(productId).isNotEqualTo(messageId);
    }
}
