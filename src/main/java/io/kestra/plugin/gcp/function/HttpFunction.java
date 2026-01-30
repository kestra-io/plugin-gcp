package io.kestra.plugin.gcp.function;

import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke an authenticated Cloud Run function",
    description = "Calls an HTTP-triggered Cloud Run service using an ID token from the provided service account and returns the response body."
)
@Plugin(examples = {
    @Example(
        full = true,
        code = """
            id: test_gcp_function
            namespace: com.company.test.gcp

            tasks:
              - id: get_hello_json
                type: io.kestra.plugin.gcp.function.HttpFunction
                httpMethod: GET
                url: https://my-function.europe-west9.run.app
            """
    )
})
public class HttpFunction extends AbstractTask implements RunnableTask<HttpFunction.Output> {

    @Schema(
        title = "HTTP method",
        description = "Rendered verb used for the request (e.g., GET, POST)"
    )
    @NotNull
    protected Property<String> httpMethod;

    @Schema(
        title = "Cloud Run URL",
        description = "Fully qualified HTTPS URL of the Cloud Run service"
    )
    @NotNull
    protected Property<String> url;

    @Schema(
        title = "HTTP body",
        description = "JSON request payload sent to the function; empty by default"
    )
    @Builder.Default
    protected Property<Map<String, Object>> httpBody = Property.ofValue(new HashMap<>());

    @Schema(
        title = "Max duration",
        description = "Maximum wait time for the HTTP call; defaults to 60 minutes"
    )
    @Builder.Default
    protected Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(60));

    @Override
    public Output run(RunContext runContext) throws Exception {
        IdTokenCredentials idTokenCredentials = IdTokenCredentials.newBuilder()
            .setIdTokenProvider((ServiceAccountCredentials) this.credentials(runContext).createScoped(runContext.render(this.scopes).asList(String.class)))
            .setTargetAudience(runContext.render(this.url).as(String.class).orElseThrow())
            .build();
        String token = idTokenCredentials.refreshAccessToken().getTokenValue();

        String rMethod = runContext.render(this.httpMethod).as(String.class).orElseThrow();
        String rUrl = runContext.render(this.url).as(String.class).orElseThrow();
        Map<String, Object> rBodyMap = runContext.render(this.httpBody).asMap(String.class, Object.class);

        Logger logger = runContext.logger();

        try (var client = new HttpClient(
            runContext,
            HttpConfiguration.builder()
                .timeout(
                    TimeoutConfiguration.builder()
                        .readIdleTimeout(Property.ofValue(Duration.ofSeconds(60)))
                        .build()
                ).build()
        )) {
            HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
                .uri(new URI(rUrl))
                .method(rMethod)
                .addHeader("Authorization", "Bearer " + token);

            logger.info("Invoking GCP HttpFunction with method='{}' url='{}'", rMethod, rUrl);

            if (!rBodyMap.isEmpty()) {
                requestBuilder.body(
                    HttpRequest.JsonRequestBody.builder()
                        .content(rBodyMap)
                        .build()
                );
            }
            HttpResponse<String> response = client.request(requestBuilder.build(), String.class);
            logger.info("HttpFunction response status: {}", response.getStatus().getCode());
            String responseBody = response.getBody() == null ? "" : response.getBody();
            try {
                return Output.builder()
                    .responseBody(JacksonMapper.ofJson().readTree(responseBody))
                    .build();
            } catch (Exception e) {
                return Output.builder()
                    .responseBody(responseBody)
                    .build();
            }
        } catch (HttpClientResponseException e) {
            logger.error("HttpFunction failed: status={}, body={}",
                e.getResponse() != null ? e.getResponse().getStatus().getCode() : -1,
                e.getResponse() != null ? e.getResponse().getBody() : "null"
            );
            throw wrapResponseException(e);
        } catch (IllegalVariableEvaluationException e) {
            logger.error("Variable evaluation error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "GCP Function response body")
        private Object responseBody;
    }

    private HttpClientResponseException wrapResponseException(HttpClientResponseException e) {
        HttpResponse<?> resp = e.getResponse();
        if (resp == null) {
            return e;
        }

        int code = resp.getStatus() != null ? resp.getStatus().getCode() : -1;
        String body = resp.getBody() != null ? String.valueOf(resp.getBody()) : "<empty>";

        String message = String.format("Request failed '%d' and body '%s'", code, body);
        return new HttpClientResponseException(message, resp, e);
    }
}
