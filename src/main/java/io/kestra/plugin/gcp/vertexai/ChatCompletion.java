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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Chat completion using the Vertex AI PaLM API for Google's PaLM 2 large language models (LLM)",
    description = "See [Generative AI quickstart using the Vertex AI API](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/api-quickstart) for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Chat completion using the Vertex AI PaLM API",
            code = {
                """
                    region: us-central1
                    projectId: my-project
                    context: I love jokes that talk about sport
                    messages:
                      - author: user
                        content: Please tell me a joke"""
            }
        )
    }
)
public class ChatCompletion extends AbstractGenerativeAi implements RunnableTask<ChatCompletion.Output> {
    private static final String MODEL_ID = "chat-bison";

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Context shapes how the model responds throughout the conversation",
        description = "For example, you can use context to specify words the model can or cannot use, topics to focus on or avoid, or the response format or style."
    )
    private String context;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "List of structured messages to the model to learn how to respond to the conversation"
    )
    private List<Example> examples;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Conversation history provided to the model in a structured alternate-author form",
        description = "Messages appear in chronological order: oldest first, newest last. When the history of messages causes the input to exceed the maximum length, the oldest messages are removed until the entire prompt is within the allowed limit."
    )
    @NotEmpty
    private List<Message> messages;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var response = call(runContext, PredictionResponse.class);
        sendMetrics(runContext, response.metadata);

        return Output.builder()
            .predictions(response.predictions)
            .build();
    }

    @Override
    protected MutableHttpRequest<?> getPredictionRequest(RunContext runContext) throws IllegalVariableEvaluationException {
        List<ChatExample> chatExamples = examples == null ? null :
            examples.stream().map(throwFunction(ex -> new ChatExample(new ChatContent(runContext.render(ex.input)), new ChatContent(runContext.render(ex.output))))).toList();
        List<Message> chatMessages = messages.stream().map(throwFunction(msg -> new Message(runContext.render(msg.author), runContext.render(msg.content)))).toList();

        var request = new ChatPromptRequest(List.of(new ChatPromptInstance(runContext.render(context), chatExamples, chatMessages)), getParameters());
        return HttpRequest.POST(getPredictionURI(runContext, MODEL_ID), request);
    }

    // request objects
    public record ChatPromptRequest(List<ChatPromptInstance> instances, ModelParameter parameters) {}
    public record ChatPromptInstance(String context, List<ChatExample> examples, List<Message> messages) {}
    public record ChatExample(ChatContent input, ChatContent output) {}
    public record ChatContent(String content) {}

    // response objects
    public record PredictionResponse(List<Prediction> predictions, Metadata metadata) {}
    public record Prediction(List<Candidate> candidates, List<CitationMetadata> citationMetadata, List<SafetyAttributes> safetyAttributes) {}
    public record Candidate(String content, String author) {}


    @Builder
    @Getter
    public static class Example {
        @PluginProperty(dynamic = true)
        @NotNull
        private String input;

        @PluginProperty(dynamic = true)
        @NotNull
        private String output;
    }

    @Builder
    @Getter
    @AllArgsConstructor
    public static class Message {
        @PluginProperty(dynamic = true)
        @NotNull
        private String author;

        @PluginProperty(dynamic = true)
        @NotNull
        private String content;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of text predictions made by the model")
        private List<Prediction> predictions;
    }
}
