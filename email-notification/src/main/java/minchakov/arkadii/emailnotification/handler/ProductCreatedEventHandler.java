package minchakov.arkadii.emailnotification.handler;

import minchakov.arkadii.common.event.ProductCreatedEvent;
import minchakov.arkadii.emailnotification.exception.NonRetryableException;
import minchakov.arkadii.emailnotification.exception.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
public class ProductCreatedEventHandler {

    private static final Logger log = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
    private final RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
        topics = "product-created-events-topic",
        containerFactory = "productCreatedEventsContainerFactory"
    )
    @RetryableTopic(
        attempts = "5",
        backOff = @BackOff(delay = 1000, multiplier = 1.5),
        exclude = NonRetryableException.class // include contains all exception classes by default
    )
    public void handleTopic(ProductCreatedEvent productCreatedEvent) {
        log.info("Received event: {}", productCreatedEvent);

        try {
            var response = restTemplate.exchange("http://localhost:8090/internalServerError", HttpMethod.GET, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Get successful response from mock service: {}", response.getBody());
            } else {
                throw new RetryableException("Strange response from mock service: " + response.getBody());
            }
        } catch (ResourceAccessException e) {
            throw new RetryableException("I/O error when requesting mock service", e);
        } catch (HttpServerErrorException e) {
            throw new NonRetryableException("Mock service error", e);
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
