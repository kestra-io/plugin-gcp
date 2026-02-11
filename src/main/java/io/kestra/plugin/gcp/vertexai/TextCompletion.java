package io.kestra.plugin.gcp.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.generativeai.ContentMaker;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Generate text with Vertex AI",
    description = "Creates text completions using the configured Vertex model and generation parameters."
)
@Plugin(
    examples = {
        @Example(
            title = "Text completion using the Vertex AI Gemini API.",
            full = true,
            code = """
                id: gcp_vertexai_text_completion
                namespace: company.team

                tasks:
                  - id: text_completion
                    type: io.kestra.plugin.gcp.vertexai.TextCompletion
                    region: us-central1
                    projectId: my-project
                    prompt: Please tell me a joke
                """
        )
    },
    metrics = {
        @Metric(
            name = "candidate.token.count",
            type = Counter.TYPE,
            description = "Number of tokens in the candidate response."
        ),
        @Metric(
            name = "prompt.token.count",
            type = Counter.TYPE,
            description = "Number of tokens in the prompt."
        ),
        @Metric(
            name = "total.token.count",
            type = Counter.TYPE,
            description = "Total number of tokens (prompt + candidate)."
        ),
        @Metric(
            name = "serialized.size",
            type = Counter.TYPE,
            unit = "bytes",
            description = "Size of the serialized metadata."
        )
    }
)
public class TextCompletion extends AbstractGenerativeAi implements RunnableTask<TextCompletion.Output> {

    @Schema(
        title = "Text input to generate model response.",
        description = "Prompts can include preamble, questions, suggestions, instructions, or examples."
    )
    @NotNull
    private Property<String> prompt;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String projectId = runContext.render(this.getProjectId()).as(String.class).orElse(null);
        String region = runContext.render(this.getRegion()).as(String.class).orElseThrow();
        String modelId = runContext.render(this.getModelId()).as(String.class).orElseThrow();

        try (VertexAI vertexAI = new VertexAI.Builder().setProjectId(projectId).setLocation(region).setCredentials(this.credentials(runContext)).build()) {
            var model = buildModel(modelId, vertexAI);
            var content = ContentMaker.fromString(runContext.render(this.prompt).as(String.class).orElseThrow());

            var response = model.generateContent(content);
            runContext.logger().debug(response.toString());

            sendMetrics(runContext, response.getUsageMetadata());

            return Output.builder()
                .predictions(response.getCandidatesList().stream().map(candidate -> Prediction.of(candidate)).toList())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of text predictions made by the model.")
        private List<Prediction> predictions;
    }
}
