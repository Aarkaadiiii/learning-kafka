package minchakov.arkadii.common.event;

import java.math.BigDecimal;

public record ProductCreatedEvent(String id, String title, BigDecimal price, long quantity) {
}
