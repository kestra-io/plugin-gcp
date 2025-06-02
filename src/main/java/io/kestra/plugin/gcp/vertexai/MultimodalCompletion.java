package io.kestra.plugin.gcp.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.Candidate;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.generativeai.ContentMaker;
import com.google.cloud.vertexai.generativeai.PartMaker;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "Use Multimodal completion using the Google Vertex AI Gemini LLM.",
    description = "See [Overview of multimodal models](https://cloud.google.com/vertex-ai/docs/generative-ai/multimodal/overview) for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Text completion using the Vertex Gemini API",
            full = true,
            code = """
                id: gcp_vertexai_multimodal_completion
                namespace: company.team

                tasks:
                  - id: multimodal_completion
                    type: io.kestra.plugin.gcp.vertexai.MultimodalCompletion
                    region: us-central1
                    projectId: my-project
                    contents:
                      - content: Please tell me a joke
                """
        ),
        @Example(
            title = "Multimodal completion using the Vertex Gemini API",
            full = true,
            code = """
                id: gcp_vertexai_multimodal_completion
                namespace: company.team

                inputs:
                  - id: image
                    type: FILE

                tasks:
                  - id: multimodal_completion
                    type: io.kestra.plugin.gcp.vertexai.MultimodalCompletion
                    region: us-central1
                    projectId: my-project
                    contents:
                      - content: Can you describe this image?
                      - mimeType: image/jpeg
                        content: "{{ inputs.image }}"
                """
        )
    }
)
public class MultimodalCompletion extends AbstractGenerativeAi implements RunnableTask<MultimodalCompletion.Output> {

    @Schema(
        title = "The chat content prompt for the model to respond to"
    )
    @PluginProperty(dynamic = true)
    @NotEmpty
    private List<Content> contents;

    @Override
    public MultimodalCompletion.Output run(RunContext runContext) throws Exception {
        String projectId = runContext.render(this.getProjectId()).as(String.class).orElse(null);
        String region = runContext.render(this.getRegion()).as(String.class).orElseThrow();
        String modelId = runContext.render(this.getModelId()).as(String.class).orElseThrow();

        try (VertexAI vertexAI = new VertexAI.Builder().setProjectId(projectId).setLocation(region).setCredentials(this.credentials(runContext)).build()) {
            var model = buildModel(modelId, vertexAI);
            var parts = contents.stream()
                .map( content -> {
                    try {
                        return content.getMimeType() == null ? runContext.render(content.getContent()).as(String.class).orElseThrow() : createPart(runContext, content);
                    } catch (IllegalVariableEvaluationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

            var response = model.generateContent(ContentMaker.fromMultiModalData(parts.toArray()));
            runContext.logger().debug(response.toString());

            sendMetrics(runContext, response.getUsageMetadata());

            var finishReason = ResponseHandler.getFinishReason(response);
            var safetyRatings = response.getCandidates(0).getSafetyRatingsList().stream()
                .map(safetyRating -> new SafetyRating(
                    safetyRating.getCategory().name(),
                    safetyRating.getProbability().name(),
                    safetyRating.getBlocked())
                )
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
        try (InputStream is = runContext.storage().getFile(URI.create(runContext.render(content.getContent()).as(String.class).orElseThrow()))) {
            byte[] partBytes = is.readAllBytes();
            return PartMaker.fromMimeTypeAndData(runContext.render(content.mimeType).as(String.class).orElseThrow(), partBytes);
        } catch (IllegalVariableEvaluationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuperBuilder
    @Value
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The generated response text"
        )
        String text;

        @Schema(
            title = "The response safety ratings"
        )
        List<SafetyRating> safetyRatings;

        @Schema(
            title = "Whether the response has been blocked for safety reasons"
        )
        boolean blocked;

        @Schema(
            title = "The reason the generation has finished"
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
        Property<String> mimeType;

        @Schema(
            title = "The content itself, should be a string for text content or a Kestra internal storage URI for other content types.",
            description = "If the content is not text, the `mimeType` property must be set."
        )
        @NotNull
        Property<String> content;
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
