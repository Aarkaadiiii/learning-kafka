package minchakov.arkadii.common.client;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class MockServiceClient {

    private final String baseUrl;
    private final RestTemplate template;

    public MockServiceClient(
        String baseUrl,
        RestTemplate template
    ) {
        this.baseUrl = baseUrl;
        this.template = template;
    }

    public ResponseEntity<String> getOk() {
        return template.exchange(baseUrl + "/ok", HttpMethod.GET, null, String.class);
    }

    public ResponseEntity<String> getInternalServerError() {
        return template.exchange(baseUrl + "/internalServerError", HttpMethod.GET, null, String.class);
    }
}
