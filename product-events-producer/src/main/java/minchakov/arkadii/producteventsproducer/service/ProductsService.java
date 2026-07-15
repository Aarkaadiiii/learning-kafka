package minchakov.arkadii.producteventsproducer.service;

import minchakov.arkadii.common.event.ProductCreatedEvent;
import minchakov.arkadii.producteventsproducer.dto.ProductCreatedDto;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
public class ProductsService {

    private static final Logger log = LoggerFactory.getLogger(ProductsService.class);
    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductsService(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String createProduct(ProductCreatedDto dto) {
        var productId = UUID.randomUUID().toString();
        var event = new ProductCreatedEvent(productId, dto.title(), dto.price(), dto.quantity());
        var eventProducerRecord = new ProducerRecord<>("product-created-events-topic", productId, event);
        eventProducerRecord.headers().add("messageId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        var cf = kafkaTemplate.send(eventProducerRecord);

        cf.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to send event '{}': {}", event, exception.getMessage());
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

        log.info("Return product id: {}", productId);
        return productId;
    }
}
