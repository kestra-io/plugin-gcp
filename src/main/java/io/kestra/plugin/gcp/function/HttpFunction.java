package io.kestra.plugin.gcp.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.NettyHttpClientFactory;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
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
    private static final Duration HTTP_READ_TIMEOUT = Duration.ofSeconds(60);
    private static final NettyHttpClientFactory FACTORY = new NettyHttpClientFactory();

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
    protected Property<Map<String, Object>> httpBody = Property.of(new HashMap<>());

    @Schema(
        title = "Max duration",
        description = "The maximum duration the task should wait until the Azure Function completion."
    )
    @Builder.Default
    protected Property<Duration> maxDuration = Property.of(Duration.ofMinutes(60));

    @Override
    public Output run(RunContext runContext) throws Exception {
        IdTokenCredentials idTokenCredentials = IdTokenCredentials.newBuilder()
            .setIdTokenProvider((ServiceAccountCredentials) this.credentials(runContext).createScoped(runContext.render(this.scopes).asList(String.class)))
            .setTargetAudience(runContext.render(this.url).as(String.class).orElseThrow())
            .build();

        String token = idTokenCredentials.refreshAccessToken().getTokenValue();

        try (HttpClient client = this.client(runContext)) {
            Mono<HttpResponse> mono = Mono.from(client.exchange(HttpRequest
                    .create(
                        HttpMethod.valueOf(runContext.render(this.httpMethod).as(String.class).orElseThrow()),
                        runContext.render(this.url).as(String.class).orElseThrow()
                    ).body(runContext.render(this.httpBody).asMap(String.class, Object.class))
                    .headers(Map.of(HttpHeaders.AUTHORIZATION, "Bearer " + token)),
                Argument.of(String.class))
            );
            HttpResponse result =  maxDuration != null ? mono.block(runContext.render(maxDuration).as(Duration.class).orElseThrow()) : mono.block();
            String body = result != null &&  result.getBody().isPresent() ? (String) result.getBody().get() : "";
            try {
                ObjectMapper mapper = new ObjectMapper();
                return Output.builder()
                    .responseBody(mapper.readTree(body))
                    .build();
            } catch (Exception e) {
                return Output.builder()
                    .responseBody(body)
                    .build();
            }
        } catch (HttpClientResponseException e) {
            throw new HttpClientResponseException(
                "Request failed '" + e.getStatus().getCode() + "' and body '" + e.getResponse().getBody(String.class).orElse("null") + "'",
                e,
                e.getResponse()
            );
        } catch (IllegalVariableEvaluationException | MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    @Builder
    static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "GCP Function response body")
        private Object responseBody;
    }

    protected HttpClient client(RunContext runContext) throws IllegalVariableEvaluationException, MalformedURLException, URISyntaxException {
        MediaTypeCodecRegistry mediaTypeCodecRegistry = ((DefaultRunContext)runContext).getApplicationContext().getBean(MediaTypeCodecRegistry.class);

        var httpConfig = new DefaultHttpClientConfiguration();
        httpConfig.setMaxContentLength(Integer.MAX_VALUE);
        httpConfig.setReadTimeout(HTTP_READ_TIMEOUT);

        DefaultHttpClient client = (DefaultHttpClient) FACTORY.createClient(URI.create(runContext.render(this.url).as(String.class).orElseThrow()).toURL(), httpConfig);
        client.setMediaTypeCodecRegistry(mediaTypeCodecRegistry);

        return client;
    }
}
