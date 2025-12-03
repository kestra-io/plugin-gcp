package io.kestra.plugin.gcp.function;

import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    title = "Run a Google Cloud Run function.",
    description = "Use this task to trigger an Cloud Run Function and collect the result."
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
    @Schema(title = "HTTP method")
    @NotNull
    protected Property<String> httpMethod;

    @Schema(title = "GCP Function URL")
    @NotNull
    protected Property<String> url;

    @Schema(
        title = "HTTP body",
        description = "JSON body of the Azure function"
    )
    @Builder.Default
    protected Property<Map<String, Object>> httpBody = Property.ofValue(new HashMap<>());

    @Schema(
        title = "Max duration",
        description = "The maximum duration the task should wait until the Azure Function completion."
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

        try(var client = new HttpClient(
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

            runContext.logger().info("Invoking GCP HttpFunction with method='{}' url='{}'", rMethod, rUrl);

            if(!rBodyMap.isEmpty()){
                requestBuilder.body(
                    HttpRequest.JsonRequestBody.builder()
                        .content(rBodyMap)
                        .build()
                );
            }
            HttpResponse<String> response = client.request(requestBuilder.build(), String.class);
            runContext.logger().info("HttpFunction response status: {}", response.getStatus().getCode());
            String responseBody = response.getBody() == null ? "" : response.getBody();
            try{
                return Output.builder()
                    .responseBody(JacksonMapper.ofJson().readTree(responseBody))
                    .build();
            } catch (Exception e) {
                return Output.builder()
                    .responseBody(responseBody)
                    .build();
            }
        } catch (HttpClientResponseException e) {
            runContext.logger().error("HttpFunction failed: status={}, body={}",
                e.getResponse() != null ? e.getResponse().getStatus().getCode() : -1,
                e.getResponse() != null ? e.getResponse().getBody() : "null"
            );
            throw wrapResponseException(e);
        }  catch (IllegalVariableEvaluationException e){
            runContext.logger().error("Variable evaluation error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

    @Getter
    @Builder
    static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "GCP Function response body")
        private Object responseBody;
    }

    private HttpClientResponseException wrapResponseException(HttpClientResponseException e) {
        HttpResponse<?> resp = e.getResponse();
        int code = -1;
        String body = "null";
        if (resp != null) {
            if (resp.getStatus() != null) {
                code = resp.getStatus().getCode();
            }
            if (resp.getBody() != null) {
                body = resp.getBody().toString();
            }
        }
        String message = "Request failed '" + code + "' and body '" + body + "'";
        return new HttpClientResponseException(message, resp, e);
    }

}
