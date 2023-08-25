package io.kestra.plugin.gcp.palm;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
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
@Schema(
    title = "TODO",
    description = "TODO"
)
@Plugin(
    examples = {
        @Example(
            title = "TODO",
            code = {
                "TODO"
            }
        )
    }
)
// https://developers.generativeai.google/api/rest/generativelanguage/models/generateText
public class GenerateText extends AbstractPalmApi implements RunnableTask<VoidOutput> {
    private static final String MODEL_ID = "text-bison-001"; //TODO multiple ones exists
    private static final String URI_PATTERN = "https://generativelanguage.googleapis.com/v1beta2/models/%s:generateText";

    @PluginProperty(dynamic = true)
    @Schema(
        title = "The free-form input text given to the model as a prompt.",
        description = "Given a prompt, the model will generate a TextCompletion response it predicts as the completion of the input text."
    )
    @NotNull
    private String prompt;

    @Builder.Default
    @Schema(
        title = "The model parameters"
    )
    @PluginProperty
    private ModelParameter parameters = ModelParameter.builder().build();

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var response = call(runContext, Candidates.class);

        return null;
    }

    protected MutableHttpRequest<TextCompletion> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException {
        var request = new TextCompletion(new Prompt(runContext.render(prompt)));
        return HttpRequest.POST(getPredictionURI(runContext, MODEL_ID), request);
    }

    protected URI getPredictionURI(RunContext runContext, String modelId) {
        var formatted = URI_PATTERN.formatted(modelId);
        runContext.logger().debug("Calling PaLM API {}", formatted);
        return URI.create(formatted);
    }

    public record TextCompletion(Prompt prompt) {}
    public record Prompt(String text) {}
    public record Candidates(List<Candidate> candidates) {}
    public record Candidate(String output, List<SafetyRating> safetyRatings) {}
    public record SafetyRating(String category, String probability) {}

    @Builder
    @Getter
    public static class ModelParameter {
        @PluginProperty
        @Schema(
            title = "A list of unique SafetySetting instances for blocking unsafe content.",
            description = "SafetySetting will be enforced on the GenerateTextRequest.prompt and GenerateTextResponse.candidates. There should not be more than one setting for each SafetyCategory type. The API will block any prompts and responses that fail to meet the thresholds set by these settings. This list overrides the default settings for each SafetyCategory specified in the safetySettings. If there is no SafetySetting for a given SafetyCategory provided in the list, the API will use the default safety setting for that category."
        )
        private List<SafetySetting> safetySettings;

        @PluginProperty
        @Schema(
            title = "The set of character sequences (up to 5) that will stop output generation",
            description = "If specified, the API will stop at the first appearance of a stop sequence. The stop sequence will not be included as part of the response."
        )
        private List<String> stopSequences;

        @Builder.Default
        @PluginProperty
        @Positive
        @Max(1)
        @Schema(
            title = "Controls the randomness of the output.",
            description = """
                Optional. Controls the randomness of the output. Note: The default value varies by model, see the Model.temperature attribute of the Model returned the getModel function.
                                
                Values can range from [0.0,1.0], inclusive. A value closer to 1.0 will produce responses that are more varied and creative, while a value closer to 0.0 will typically result in more straightforward responses from the model."""
        )
        private Float temperature = 0.2F;

        @Builder.Default
        @PluginProperty
        @Positive
        @Schema(
            title = "Number of generated responses to return.",
            description = "This value must be between [1, 8], inclusive. If unset, this will default to 1."
        )
        @Min(1)
        @Max(8)
        private Integer candidateCount = 1;

        @Builder.Default
        @PluginProperty
        @Min(1)
        @Max(1024)
        @Schema(
            title = "The maximum number of tokens to include in a candidate.",
            description = "If unset, this will default to outputTokenLimit specified in the Model specification."
        )
        private Integer maxOutputTokens = 128;

        @Builder.Default
        @PluginProperty
        @Min(1)
        @Max(40)
        @Schema(
            title = "The maximum number of tokens to consider when sampling.",
            description = """
                The model uses combined Top-k and nucleus sampling.
                                
                Top-k sampling considers the set of topK most probable tokens. Defaults to 40.
                                
                Note: The default value varies by model, see the Model.top_k attribute of the Model returned the getModel function."""
        )
        private Integer topK = 40;

        @Builder.Default
        @PluginProperty
        @Positive
        @Max(1)
        @Schema(
            title = "The maximum cumulative probability of tokens to consider when sampling.",
            description = """
                The model uses combined Top-k and nucleus sampling.
                                
                Tokens are sorted based on their assigned probabilities so that only the most liekly tokens are considered. Top-k sampling directly limits the maximum number of tokens to consider, while Nucleus sampling limits number of tokens based on the cumulative probability.
                                
                Note: The default value varies by model, see the Model.top_p attribute of the Model returned the getModel function."""
        )
        private Float topP = 0.95F;

        @Builder
        @Getter
        public static class SafetySetting {
            @NotNull
            @PluginProperty
            @Schema(
                title = "Safety category."
            )
            private String category;

            @NotNull
            @PluginProperty
            @Schema(
                title = "Safety threshold."
            )
            private String threshold;
        }
    }
}
