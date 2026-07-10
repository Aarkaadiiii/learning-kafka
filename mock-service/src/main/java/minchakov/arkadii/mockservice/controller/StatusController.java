package minchakov.arkadii.mockservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {

    @GetMapping("/ok")
    public ResponseEntity<String> ok() {
        return ResponseEntity.ok("Request received successfully");
    }

    @GetMapping("/internalServerError")
    public ResponseEntity<String> internalServerError() {
        return ResponseEntity.internalServerError().body("Request processed with internal server error");
    }
}
