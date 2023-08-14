package io.kestra.plugin.gcp.vertexai;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.NettyHttpClientFactory;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractGenerativeAi extends AbstractTask {
    private static final NettyHttpClientFactory FACTORY = new NettyHttpClientFactory();
    private static final String URI_PATTERN = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

    @Schema(
        title = "The region"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String region;

    @Builder.Default
    @Schema(
        title = "The model parameters"
    )
    @PluginProperty
    private ModelParameter parameters = ModelParameter.builder().build();

    <T> T call(RunContext runContext, Class<T> responseClass) {
        try {
            var auth = credentials(runContext);
            auth.refreshIfExpired();

            var request = getPredictionRequest(runContext)
                .contentType(MediaType.APPLICATION_JSON)
                .bearerAuth(auth.getAccessToken().getTokenValue());

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

    protected abstract MutableHttpRequest<?> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException;

    protected void sendMetrics(RunContext runContext, Metadata metadata) {
        runContext.metric(Counter.of("input.token.total.tokens", metadata.tokenMetadata.inputTokenCount.totalTokens));
        runContext.metric(Counter.of("input.token.total.billable.characters", metadata.tokenMetadata.inputTokenCount.totalBillableCharacters));
        runContext.metric(Counter.of("output.token.total.tokens", metadata.tokenMetadata.outputTokenCount.totalTokens));
        runContext.metric(Counter.of("output.token.total.billable.characters", metadata.tokenMetadata.outputTokenCount.totalBillableCharacters));
    }

    protected URI getPredictionURI(RunContext runContext, String modelId) throws IllegalVariableEvaluationException {
        var formatted = URI_PATTERN.formatted(runContext.render(getRegion()), runContext.render(getProjectId()), runContext.render(getRegion()), modelId);
        runContext.logger().debug("Calling Vertex.AI prediction API {}", formatted);
        return URI.create(formatted);
    }

    private HttpClient client(RunContext runContext) throws MalformedURLException {
        MediaTypeCodecRegistry mediaTypeCodecRegistry = runContext.getApplicationContext().getBean(MediaTypeCodecRegistry.class);
        var httpConfig = new DefaultHttpClientConfiguration();
        httpConfig.setMaxContentLength(Integer.MAX_VALUE);

        DefaultHttpClient client = (DefaultHttpClient) FACTORY.createClient(null, httpConfig);
        client.setMediaTypeCodecRegistry(mediaTypeCodecRegistry);
        return client;
    }

    @Builder
    @Getter
    public static class ModelParameter {
        @Builder.Default
        @PluginProperty
        @Positive
        @Max(1)
        @Schema(
            title = "Temperature used for sampling during the response generation, which occurs when topP and topK are applied.",
            description = "Temperature controls the degree of randomness in token selection. Lower temperatures are good for prompts that require a more deterministic and less open-ended or creative response, while higher temperatures can lead to more diverse or creative results. A temperature of 0 is deterministic: the highest probability response is always selected. For most use cases, try starting with a temperature of 0.2."
        )
        private Float temperature = 0.2F;

        @Builder.Default
        @PluginProperty
        @Min(1)
        @Max(1024)
        @Schema(
            title = "Maximum number of tokens that can be generated in the response",
            description = """
                Specify a lower value for shorter responses and a higher value for longer responses.
                A token may be smaller than a word. A token is approximately four characters. 100 tokens correspond to roughly 60-80 words."""
        )
        private Integer maxOutputTokens = 128;

        @Builder.Default
        @PluginProperty
        @Min(1)
        @Max(40)
        @Schema(
            title = "Top-k changes how the model selects tokens for output",
            description = """
                A top-k of 1 means the selected token is the most probable among all tokens in the model's vocabulary (also called greedy decoding), while a top-k of 3 means that the next token is selected from among the 3 most probable tokens (using temperature).
                For each token selection step, the top K tokens with the highest probabilities are sampled. Then tokens are further filtered based on topP with the final token selected using temperature sampling.
                Specify a lower value for less random responses and a higher value for more random responses."""
        )
        private Integer topK = 40;

        @Builder.Default
        @PluginProperty
        @Positive
        @Max(1)
        @Schema(
            title = "Top-p changes how the model selects tokens for output",
            description = """
                Tokens are selected from most K (see topK parameter) probable to least until the sum of their probabilities equals the top-p value. For example, if tokens A, B, and C have a probability of 0.3, 0.2, and 0.1 and the top-p value is 0.5, then the model will select either A or B as the next token (using temperature) and doesn't consider C. The default top-p value is 0.95.
                Specify a lower value for less random responses and a higher value for more random responses."""
        )
        private Float topP = 0.95F;
    }

    // common response objects
    public record CitationMetadata(List<Citation> citations) {}
    public record Citation(List<String> citations) {}
    public record SafetyAttributes(List<Float> scores, List<String> categories, Boolean blocked) {}
    public record Metadata(TokenMetadata tokenMetadata) {}
    public record TokenMetadata(TokenCount outputTokenCount, TokenCount inputTokenCount) {}
    public record TokenCount(Integer totalTokens, Integer totalBillableCharacters) {}
}
