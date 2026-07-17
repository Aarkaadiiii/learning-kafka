package minchakov.arkadii.common.config;

import minchakov.arkadii.common.client.MockServiceClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class MockServiceClientConfiguration {

    private static final String MOCK_SERVICE_BASE_URL = "http://localhost:8090";

    @Bean
    public RestTemplate mockServiceRestTemplate() {
        return new RestTemplate();
    }

    @Bean
    public MockServiceClient mockServiceClient() {
        return new MockServiceClient(MOCK_SERVICE_BASE_URL, mockServiceRestTemplate());
    }
}
