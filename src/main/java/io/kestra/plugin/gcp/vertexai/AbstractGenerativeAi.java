package io.kestra.plugin.gcp.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.*;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractGenerativeAi extends AbstractTask {
    private static final String URI_PATTERN = "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

    @Schema(
        title = "The GCP region."
    )
    @NotNull
    private Property<String> region;

    @Builder.Default
    @Schema(
        title = "The model parameters."
    )
    @PluginProperty
    private ModelParameter parameters = ModelParameter.builder().build();

    protected GenerativeModel buildModel(String modelName, VertexAI vertexAI) {
        GenerativeModel model = new GenerativeModel(modelName, vertexAI);
        if (this.getParameters() != null) {
            var config = GenerationConfig.newBuilder();
            config.setTemperature(this.getParameters().getTemperature());
            config.setMaxOutputTokens(this.getParameters().getMaxOutputTokens());
            config.setTopK(this.getParameters().getTopK());
            config.setTopP(this.getParameters().getTopP());
            model.withGenerationConfig(config.build());
        }
        return model;
    }

    protected void sendMetrics(RunContext runContext, GenerateContentResponse.UsageMetadata metadata) {
        runContext.metric(Counter.of("candidate.token.count", metadata.getCandidatesTokenCount()));
        runContext.metric(Counter.of("prompt.token.count", metadata.getPromptTokenCount()));
        runContext.metric(Counter.of("total.token.count", metadata.getTotalTokenCount()));
        runContext.metric(Counter.of("serialized.size", metadata.getSerializedSize()));
    }

    protected void sendMetrics(RunContext runContext, List<GenerateContentResponse.UsageMetadata> metadatas) {
        runContext.metric(Counter.of("candidate.token.count", metadatas.stream().mapToInt(metadata -> metadata.getCandidatesTokenCount()).sum()));
        runContext.metric(Counter.of("prompt.token.count", metadatas.stream().mapToInt(metadata -> metadata.getPromptTokenCount()).sum()));
        runContext.metric(Counter.of("total.token.count", metadatas.stream().mapToInt(metadata -> metadata.getTotalTokenCount()).sum()));
        runContext.metric(Counter.of("serialized.size", metadatas.stream().mapToInt(metadata -> metadata.getSerializedSize()).sum()));
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
            title = "Maximum number of tokens that can be generated in the response.",
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
            title = "Top-k changes how the model selects tokens for output.",
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
            title = "Top-p changes how the model selects tokens for output.",
            description = """
                Tokens are selected from most K (see topK parameter) probable to least until the sum of their probabilities equals the top-p value. For example, if tokens A, B, and C have a probability of 0.3, 0.2, and 0.1 and the top-p value is 0.5, then the model will select either A or B as the next token (using temperature) and doesn't consider C. The default top-p value is 0.95.
                Specify a lower value for less random responses and a higher value for more random responses."""
        )
        private Float topP = 0.95F;
    }

    // common response objects
    public record Prediction(SafetyAttributes safetyAttributes, CitationMetadata citationMetadata, String content) {
        public static Prediction of(Candidate candidate) {
            return new Prediction(SafetyAttributes.of(candidate.getSafetyRatingsList()),
                CitationMetadata.of(candidate.getCitationMetadata()),
                candidate.getContent().getParts(0).getText()
            );
        }
    }
    public record CitationMetadata(List<Citation> citations) {
        public static CitationMetadata of(com.google.cloud.vertexai.api.CitationMetadata citationMetadata) {
            return new CitationMetadata(
                citationMetadata.getCitationsList().stream().map(citation -> new Citation(List.of(citation.getTitle()))).toList()
            );
        }
    }
    public record Citation(List<String> citations) {}
    public record SafetyAttributes(List<Float> scores, List<String> categories, Boolean blocked) {
        public static SafetyAttributes of(List<SafetyRating> safetyRatingsList) {
            return new SafetyAttributes(
                safetyRatingsList.stream().map(safetyRating -> safetyRating.getSeverityScore()).toList(),
                safetyRatingsList.stream().map(safetyRating -> safetyRating.getCategory().name()).toList(),
                safetyRatingsList.stream().anyMatch(safetyRating -> safetyRating.getBlocked())
            );
        }
    }
}
