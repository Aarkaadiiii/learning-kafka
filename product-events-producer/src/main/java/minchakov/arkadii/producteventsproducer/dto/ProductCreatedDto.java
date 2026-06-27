package minchakov.arkadii.producteventsproducer.dto;

import java.math.BigDecimal;

public record ProductCreatedDto(String title, BigDecimal price, long quantity) {
}
