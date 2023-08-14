package io.kestra.plugin.gcp.vertexai;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.swagger.v3.oas.annotations.media.Schema;
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
    title = "Text completion using the Vertex AI PaLM API for Google's PaLM 2 large language models (LLM)",
    description = "See [Generative AI quickstart using the Vertex AI API](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/api-quickstart) for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Text completion using the Vertex AI PaLM API",
            code = {
                """
                    region: us-central1
                    projectId: my-project
                    prompt: Please tell me a joke"""
            }
        )
    }
)
public class TextCompletion extends AbstractGenerativeAi implements RunnableTask<TextCompletion.Output> {
    private static final String MODEL_ID = "text-bison";

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Text input to generate model response",
        description = "Prompts can include preamble, questions, suggestions, instructions, or examples."
    )
    private String prompt;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var response = call(runContext, PredictionResponse.class);
        sendMetrics(runContext, response.metadata);

        return Output.builder()
            .predictions(response.predictions)
            .build();
    }

    @Override
    protected MutableHttpRequest<TextPromptRequest> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException {
        var request = new TextPromptRequest(List.of(new TextPromptInstance(runContext.render(prompt))), getParameters());
        return HttpRequest.POST(getPredictionURI(runContext, MODEL_ID), request);
    }

    // request objects
    public record TextPromptRequest(List<TextPromptInstance> instances, ModelParameter parameters) {}
    public record TextPromptInstance(String prompt) {}

    // response objects
    public record PredictionResponse(List<Prediction> predictions, Metadata metadata) {}
    public record Prediction(SafetyAttributes safetyAttributes, CitationMetadata citationMetadata, String content) {}

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of text predictions made by the model")
        private List<Prediction> predictions;
    }
}
