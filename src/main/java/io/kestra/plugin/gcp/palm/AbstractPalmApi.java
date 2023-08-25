package io.kestra.plugin.gcp.palm;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.NettyHttpClientFactory;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPalmApi {
    private static final NettyHttpClientFactory FACTORY = new NettyHttpClientFactory();

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The PaLM API key."
    )
    @NotNull
    private String apiKey;

    <T> T call(RunContext runContext, Class<T> responseClass) {
        try {
            var request = getPredictionRequest(runContext)
                .contentType(MediaType.APPLICATION_JSON)
                .header("x-goog-api-key", runContext.render(apiKey));

            try (HttpClient client = this.client(runContext)) {
                var response = client.toBlocking().exchange(request, responseClass);
                var predictionResponse = response.body();
                if (predictionResponse == null) {
                    throw new RuntimeException("Received an empty response from the Vertex.ai prediction API");
                }

                return predictionResponse;
            }
        } catch (HttpClientResponseException e) {
            throw new HttpClientResponseException(
                "Request failed '" + e.getStatus().getCode() + "' and body '" + e.getResponse().getBody(String.class).orElse("null") + "'",
                e,
                e.getResponse()
            );
        } catch (IllegalVariableEvaluationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpClient client(RunContext runContext) throws MalformedURLException {
        MediaTypeCodecRegistry mediaTypeCodecRegistry = runContext.getApplicationContext().getBean(MediaTypeCodecRegistry.class);
        var httpConfig = new DefaultHttpClientConfiguration();
        httpConfig.setMaxContentLength(Integer.MAX_VALUE);

        DefaultHttpClient client = (DefaultHttpClient) FACTORY.createClient(null, httpConfig);
        client.setMediaTypeCodecRegistry(mediaTypeCodecRegistry);
        return client;
    }

    protected abstract MutableHttpRequest<?> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException;
}
