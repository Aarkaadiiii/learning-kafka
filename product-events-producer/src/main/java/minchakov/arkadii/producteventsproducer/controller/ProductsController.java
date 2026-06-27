package minchakov.arkadii.producteventsproducer.controller;

import minchakov.arkadii.producteventsproducer.dto.ProductCreatedDto;
import minchakov.arkadii.producteventsproducer.service.ProductsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/products")
public class ProductsController {

    private final ProductsService productsService;

    public ProductsController(ProductsService productsService) {
        this.productsService = productsService;
    }

    @PostMapping
    public ResponseEntity<String> createProduct(
        @RequestBody
        ProductCreatedDto productCreatedDto
    ) {
        var productId = productsService.createProduct(productCreatedDto);
        return ResponseEntity.created(URI.create("/products/" + productId)).body(productId);
    }
}
