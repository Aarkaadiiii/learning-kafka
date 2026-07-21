package minchakov.arkadii.emailnotification.handler;

import minchakov.arkadii.common.client.MockServiceClient;
import minchakov.arkadii.common.config.KafkaConsumerConfigurationProperties;
import minchakov.arkadii.common.event.NotificationSentEvent;
import minchakov.arkadii.common.event.ProductCreatedEvent;
import minchakov.arkadii.emailnotification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@EmbeddedKafka
@ActiveProfiles("test")
@ExtendWith(MockitoExtension.class)
@EnableConfigurationProperties(KafkaConsumerConfigurationProperties.class)
@DisplayName("Integration tests for consumer of product-created-events-topic")
public class ProductCreatedEventHandlerIntegrationTests {

    @MockitoSpyBean
    private ProductCreatedEventHandler handler;

    @Autowired
    private ProcessedEventRepository repository;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @MockitoBean
    private MockServiceClient client;

    @Autowired
    private NewTopic notificationSentEventsTopic;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaConsumerConfigurationProperties props;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String consumerAutoOffsetReset;

    private ConsumerFactory<String, Object> consumerFactory;

    private ContainerProperties containerProperties;

    private ConcurrentLinkedQueue<ConsumerRecord<String, Object>> events;

    private KafkaMessageListenerContainer<String, Object> container;

    private boolean isInitialized = false;

    @BeforeEach
    void setUp() {
        if (!isInitialized) {
            var configs = new HashMap<String, Object>();
            configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            configs.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
            configs.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, props.trustedPackages());
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
            configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffsetReset);

            consumerFactory = new DefaultKafkaConsumerFactory<>(configs);

            containerProperties = new ContainerProperties(notificationSentEventsTopic.name());
            events = new ConcurrentLinkedQueue<>();
            containerProperties.setMessageListener((MessageListener<String, Object>) events::add);

            isInitialized = true;
        }

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.start();
        ContainerTestUtils.waitForAssignment(container, notificationSentEventsTopic.numPartitions());
    }

    @AfterEach
    void tearDown() {
        container.stop();
        events.clear();
    }

    @Test
    @DisplayName("Should save event to database and send event to notifications-sent-events-topic when event is valid")
    void handleTopic_shouldSaveToDBAndSendToTopic_whenEverythingIsFine()
    throws ExecutionException, InterruptedException {
        // Arrange
        var productCreatedEvent = new ProductCreatedEvent(
            UUID.randomUUID().toString(),
            "Samsung Galaxy Z Fold5",
            BigDecimal.valueOf(100),
            1
        );
        var messageId = UUID.randomUUID().toString();
        var producerRecord = new ProducerRecord<String, Object>(props.topic().name(), productCreatedEvent.id(), productCreatedEvent);
        producerRecord.headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));

        when(client.getOk()).thenReturn(ResponseEntity.ok("OK"));

        var productCreatedEventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);
        var messageIdCaptor = ArgumentCaptor.forClass(String.class);
        var messageKeyCaptor = ArgumentCaptor.forClass(String.class);

        // Act
        kafkaTemplate.send(producerRecord).get();
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> {
                   try {
                       verify(handler).handleTopic(
                           productCreatedEventCaptor.capture(),
                           messageIdCaptor.capture(),
                           messageKeyCaptor.capture()
                       );
                       return true;
                   } catch (Throwable t) {
                       return false;
                   }
               });

        // Assert
        assertThat(productCreatedEventCaptor.getValue()).isEqualTo(productCreatedEvent);
        assertThat(messageIdCaptor.getValue()).isEqualTo(messageId);
        assertThat(messageKeyCaptor.getValue()).isEqualTo(productCreatedEvent.id());

        await().atMost(5, TimeUnit.SECONDS).until(() -> !events.isEmpty());
        var sentEvent = events.poll();

        assertThat(sentEvent).isNotNull();
        assertThat(sentEvent.value()).isInstanceOf(NotificationSentEvent.class);

        var notificationSentEvent = (NotificationSentEvent) sentEvent.value();
        assertThat(notificationSentEvent).isNotNull();
        assertThat(notificationSentEvent.id()).isEqualTo(sentEvent.key());
        assertThat(notificationSentEvent.type()).isEqualTo(NotificationSentEvent.Type.EMAIL);
        assertThat(notificationSentEvent.productId()).isEqualTo(productCreatedEvent.id());

        var savedEventOpt = repository.findById(messageId);
        assertThat(savedEventOpt).isPresent();

        var savedEvent = savedEventOpt.get();
        assertThat(savedEvent.getMessageId()).isEqualTo(messageId);
        assertThat(savedEvent.getProductId()).isEqualTo(productCreatedEvent.id());
    }
}
