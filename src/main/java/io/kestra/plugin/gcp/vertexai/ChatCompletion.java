package io.kestra.plugin.gcp.vertexai;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.ContentMaker;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.List;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Chat completion using the Vertex AI for Google's Gemini large language models (LLM).",
    description = "See [Generative AI quickstart using the Vertex AI API](https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/api-quickstart) for more information."
)
@Plugin(
    examples = {
        @Example(
            title = "Chat completion using the Vertex AI Gemini API.",
            full = true,
            code = """
                id: gcp_vertexai_chat_completion
                namespace: company.team

                tasks:
                  - id: chat_completion
                    type: io.kestra.plugin.gcp.vertexai.ChatCompletion
                    region: us-central1
                    projectId: my-project
                    context: I love jokes that talk about sport
                    messages:
                      - author: user
                        content: Please tell me a joke
                """
        )
    }
)
public class ChatCompletion extends AbstractGenerativeAi implements RunnableTask<ChatCompletion.Output> {
    private static final String MODEL_ID = "gemini-pro";

    @PluginProperty(dynamic = true)
    @Schema(
        title = "For backward compatibility, since migration to Gemini LLM this property will be the first message to be send to the chat."
    )
    @Deprecated
    private String context;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "This property is not used anymore since migration to Gemini LLM."
    )
    @Deprecated
    private List<Example> examples;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Chat messages.",
        description = "Messages appear in chronological order: oldest first, newest last. When the history of messages causes the input to exceed the maximum length, the oldest messages are removed until the entire prompt is within the allowed limit."
    )
    @NotEmpty
    private List<Message> messages;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "Conversation history provided to the model.",
        description = "Messages appear in chronological order: oldest first, newest last. When the history of messages causes the input to exceed the maximum length, the oldest messages are removed until the entire prompt is within the allowed limit."
    )
    private List<Message> history;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String projectId = runContext.render(this.getProjectId()).as(String.class).orElse(null);
        String region = runContext.render(this.getRegion()).as(String.class).orElseThrow();

        try (VertexAI vertexAI = new VertexAI.Builder().setProjectId(projectId).setLocation(region).setCredentials(this.credentials(runContext)).build()) {
            var model = buildModel(MODEL_ID, vertexAI);
            var chatSession = model.startChat();

            if (history != null) {
                List<com.google.cloud.vertexai.api.Content> historyContents = history.stream()
                    .map(throwFunction(message -> ContentMaker.fromString(runContext.render(message.content).as(String.class).orElseThrow())))
                    .toList();
                chatSession.setHistory(historyContents);
            }

            if (context != null) {
                chatSession.sendMessage(runContext.render(context));
            }

            List<GenerateContentResponse> responses = messages.stream()
                .map(throwFunction(message -> chatSession.sendMessage(runContext.render(message.content).as(String.class).orElseThrow())))
                .toList();


            List<com.google.cloud.vertexai.api.Candidate> candidates = responses.stream().flatMap(response -> response.getCandidatesList().stream()).toList();
            List<GenerateContentResponse.UsageMetadata> metadatas = responses.stream().map(response -> response.getUsageMetadata()).toList();

            sendMetrics(runContext, metadatas);

            return Output.builder()
                .predictions(candidates.stream().map(candidate -> AbstractGenerativeAi.Prediction.of(candidate)).toList())
                .build();
        }
    }

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
        @Schema(title = "This property is not used anymore since migration to Gemini LLM")
        @Deprecated
        @PluginProperty(dynamic = true)
        private String author;

        @NotNull
        private Property<String> content;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of text predictions made by the model.")
        private List<Prediction> predictions;
    }
}
