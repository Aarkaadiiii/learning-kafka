package minchakov.arkadii.firstmicroservice.dto;

import java.math.BigDecimal;

public record ProductCreatedDto(String title, BigDecimal price, long quantity) {
}
