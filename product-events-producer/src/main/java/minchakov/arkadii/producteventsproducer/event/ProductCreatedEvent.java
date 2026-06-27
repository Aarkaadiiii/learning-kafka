package minchakov.arkadii.producteventsproducer.event;

import java.math.BigDecimal;

public record ProductCreatedEvent(String id, String title, BigDecimal price, long quantity) {
}
