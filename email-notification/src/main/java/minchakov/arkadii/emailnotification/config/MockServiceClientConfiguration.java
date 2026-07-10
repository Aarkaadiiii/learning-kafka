package minchakov.arkadii.emailnotification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class MockServiceClientConfiguration {

    @Bean
    public RestTemplate mockServiceRestTemplate() {
        return new RestTemplate();
    }
}
