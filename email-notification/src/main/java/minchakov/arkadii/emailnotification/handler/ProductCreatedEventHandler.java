package minchakov.arkadii.emailnotification.handler;

import minchakov.arkadii.common.client.MockServiceClient;
import minchakov.arkadii.common.event.NotificationSentEvent;
import minchakov.arkadii.common.event.ProductCreatedEvent;
import minchakov.arkadii.emailnotification.exception.NonRetryableException;
import minchakov.arkadii.emailnotification.exception.RetryableException;
import minchakov.arkadii.emailnotification.model.ProcessedEvent;
import minchakov.arkadii.emailnotification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.util.UUID;

@Component
public class ProductCreatedEventHandler {

    private static final Logger log = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
    private final MockServiceClient mockServiceClient;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, NotificationSentEvent> kafkaTemplate;
    private final NewTopic notificationSentEventsTopic;

    public ProductCreatedEventHandler(
        MockServiceClient mockServiceClient,
        ProcessedEventRepository processedEventRepository,
        KafkaTemplate<String, NotificationSentEvent> kafkaTemplate,
        NewTopic notificationSentEventsTopic
    ) {
        this.mockServiceClient = mockServiceClient;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.notificationSentEventsTopic = notificationSentEventsTopic;
    }

    @Transactional("transactionManager")
    @KafkaListener(
        topics = "${spring.kafka.consumer.topic.name}",
        containerFactory = "productCreatedEventsContainerFactory"
    )
    @RetryableTopic(
        attempts = "5",
        backOff = @BackOff(delay = 1000, multiplier = 1.5),
        exclude = NonRetryableException.class, // include contains all exception classes by default
        kafkaTemplate = "productCreatedEventsDLTKafkaTemplate"
    )
    public void handleTopic(
        ProductCreatedEvent productCreatedEvent,
        @Header("messageId") String messageId,
        @Header(KafkaHeaders.RECEIVED_KEY) String messageKey
    ) {
        log.info("Received event: {}", productCreatedEvent);

        if (processedEventRepository.existsById(messageId)) {
            log.info("Received duplicate message with id {} and key {}, updating offset", messageId, messageKey);
            return;
        }

        try {
            var notificationSentEvent = new NotificationSentEvent(
                UUID.randomUUID().toString(),
                NotificationSentEvent.Type.EMAIL,
                productCreatedEvent.id()
            );

            var cf = kafkaTemplate.send(notificationSentEventsTopic.name(), notificationSentEvent.id(), notificationSentEvent);

            cf.whenComplete((result, exception) -> {
               if (exception != null) {
                   log.error("Failed to send event {}: {}", notificationSentEvent, exception.getMessage());
               } else {
                   var metadata = result.getRecordMetadata();
                   log.info(
                       "Sent event {} with headers {} to topic {}, partition {}, it has offset {}",
                       result.getProducerRecord().value(),
                       result.getProducerRecord().headers(),
                       metadata.topic(),
                       metadata.partition(),
                       metadata.offset()
                   );
               }
            });

            var response = mockServiceClient.getOk();
//            var response = mockServiceClient.getInternalServerError();
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Get successful response from mock service: {}", response.getBody());
            } else {
                throw new RetryableException("Strange response from mock service: " + response.getBody());
            }

            processedEventRepository.save(new ProcessedEvent(messageId, productCreatedEvent.id()));

//            mockServiceClient.getInternalServerError();
        } catch (ResourceAccessException e) {
            throw new RetryableException("I/O error when requesting mock service", e);
        } catch (HttpServerErrorException e) {
            throw new NonRetryableException("Mock service error", e);
        } catch (DataIntegrityViolationException e) {
            throw new NonRetryableException("Exception while saving processed event", e);
        } catch (Exception e) {
            throw new NonRetryableException("Unexpected exception", e);
        }

        log.info("Processed event with id: {}", productCreatedEvent.id());
    }

    @DltHandler
    public void handleDlt(
        ProductCreatedEvent productCreatedEvent,
        @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false)
        String exceptionMessage
    ) {
        log.info("DLT event {} got exception: {}", productCreatedEvent, exceptionMessage);
    }
}
