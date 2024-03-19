package io.kestra.plugin.gcp.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.Candidate;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.GenerationConfig;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.generativeai.ContentMaker;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.PartMaker;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.MutableHttpRequest;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Multimodal completion using the Vertex AI Gemini large language models (LLM).",
    description = "See [Overview of multimodal models](https://cloud.google.com/vertex-ai/docs/generative-ai/multimodal/overview) for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Text completion using the Vertex Gemini API",
            code = {
                """
                    region: us-central1
                    projectId: my-project
                    contents:
                      - content: Please tell me a joke"""
            }),
        @Example(
            title = "Multimodal completion using the Vertex Gemini API",
            code = {
                """
                    region: us-central1
                    projectId: my-project
                    contents:
                      - content: Can you describe this image?
                      - mimeType: image/jpeg
                        content: "{{ inputs.image }}"
                """
            })
    }
)
public class MultimodalCompletion extends AbstractGenerativeAi implements RunnableTask<MultimodalCompletion.Output> {
    private static final String MODEL_ID = "gemini-pro-vision";


    @Schema(
        title = "The contents."
    )
    @PluginProperty(dynamic = true)
    @NotEmpty
    private List<Content> contents;

    @Override
    public MultimodalCompletion.Output run(RunContext runContext) throws Exception {
        String projectId = runContext.render(this.getProjectId());
        String region = runContext.render(this.getRegion());

        try (VertexAI vertexAI = new VertexAI(projectId, region, this.credentials(runContext))) {
            var parts = contents.stream().map( content -> content.getMimeType() == null ? content.getContent() : createPart(runContext, content)).toList();

            GenerativeModel model = new GenerativeModel(MODEL_ID, vertexAI);
            if (this.getParameters() != null) {
                var config = GenerationConfig.newBuilder();
                config.setTemperature(this.getParameters().getTemperature());
                config.setMaxOutputTokens(this.getParameters().getMaxOutputTokens());
                config.setTopK(this.getParameters().getTopK());
                config.setTopP(this.getParameters().getTopP());
                model.setGenerationConfig(config.build());
            }

            GenerateContentResponse response = model.generateContent(ContentMaker.fromMultiModalData(parts.toArray()));
            runContext.logger().debug(response.toString());

            runContext.metric(Counter.of("candidate.token.count", response.getUsageMetadata().getCandidatesTokenCount()));
            runContext.metric(Counter.of("prompt.token.count", response.getUsageMetadata().getPromptTokenCount()));
            runContext.metric(Counter.of("total.token.count", response.getUsageMetadata().getTotalTokenCount()));
            runContext.metric(Counter.of("serialized.size", response.getUsageMetadata().getSerializedSize()));

            var finishReason = ResponseHandler.getFinishReason(response);
            var safetyRatings = response.getCandidates(0).getSafetyRatingsList().stream()
                .map(safetyRating -> new SafetyRating(safetyRating.getCategory().name(), safetyRating.getProbability().name(), safetyRating.getBlocked()))
                .toList();
            var output = Output.builder()
                .finishReason(finishReason.name())
                .safetyRatings(safetyRatings);

            if (finishReason == Candidate.FinishReason.SAFETY) {
                runContext.logger().warn("Content response has been blocked for safety reason");
                output.blocked(true);
            }
            else if (finishReason == Candidate.FinishReason.RECITATION) {
                runContext.logger().warn("Content response has been blocked for recitation reason");
                output.blocked(true);
            }
            else {
                output.text(ResponseHandler.getText(response));
            }

            return output.build();
        }
    }

    private Part createPart(RunContext runContext, Content content) {
        try (InputStream is = runContext.storage().getFile(URI.create(runContext.render(content.getContent())))) {
            byte[] partBytes = is.readAllBytes();
            return PartMaker.fromMimeTypeAndData(content.mimeType, partBytes);
        } catch (IllegalVariableEvaluationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected MutableHttpRequest<?> getPredictionRequest(RunContext runContext) {
        throw new UnsupportedOperationException("Generative AI task didn't use the request facility");
    }

    @SuperBuilder
    @Value
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The generated response text."
        )
        String text;

        @Schema(
            title = "The response safety ratings."
        )
        List<SafetyRating> safetyRatings;

        @Schema(
            title = "Whether the response has been blocked for safety reasons."
        )
        boolean blocked;

        @Schema(
            title = "The reason the generation has finished."
        )
        String finishReason;

        @Override
        public Optional<State.Type> finalState() {
            return blocked ? Optional.of(State.Type.WARNING) : io.kestra.core.models.tasks.Output.super.finalState();
        }
    }

    @Value
    public static class Content {
        @Schema(
            title = "Mime type of the content, use it only when the content is not text."
        )
        @PluginProperty(dynamic = true)
        String mimeType;

        @Schema(
            title = "The content itself, should be a string for text content or a Kestra internal storage URI for other content types.",
            description = "If the content is not text, the `mimeType` property must be set."
        )
        @PluginProperty
        @NotNull
        String content;
    }

    @Value
    public static class SafetyRating {
        @Schema(
            title = "Safety category."
        )
        String category;

        @Schema(
            title = "Safety rating probability."
        )
        String probability;

        @Schema(
            title = "Whether the response has been blocked for safety reasons."
        )
        boolean blocked;
    }
}
