package io.kestra.plugin.gcp.vertexai;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.aiplatform.v1.EndpointName;
import com.google.cloud.aiplatform.v1.PredictRequest;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;
import io.kestra.plugin.gcp.vertexai.models.TaskType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Generate text embeddings with Google Cloud Vertex AI",
    description = """
        Generates vector embeddings for a single text or batch of texts using Vertex AI foundation models (e.g., text-embedding-005, text-multilingual-embedding-002).
        Supports downstream task types, document titles, and customizable output dimensionality for vector search and RAG workflows.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Generate an embedding for a single search query",
            full = true,
            code = """
                id: gcp_vertexai_query_embedding
                namespace: company.ai

                tasks:
                  - id: embed_query
                    type: io.kestra.plugin.gcp.vertexai.Embeddings
                    projectId: my-gcp-project
                    region: us-central1
                    modelId: text-embedding-005
                    text: "How to configure GCS lifecycle policies?"
                    taskType: RETRIEVAL_QUERY
                    outputDimensionality: 768
                """
        ),
        @Example(
            title = "Generate embeddings for batch document chunks",
            full = true,
            code = """
                id: gcp_vertexai_batch_embeddings
                namespace: company.ai

                tasks:
                  - id: embed_chunks
                    type: io.kestra.plugin.gcp.vertexai.Embeddings
                    projectId: my-gcp-project
                    region: us-central1
                    modelId: text-embedding-005
                    taskType: RETRIEVAL_DOCUMENT
                    title: "GCS Architecture"
                    texts:
                      - "Cloud Storage is an object storage service."
                      - "Lifecycle rules automate transitions between storage classes."
                """
        )
    },
    metrics = {
        @Metric(
            name = "embeddings.count",
            type = Counter.TYPE,
            description = "Number of embeddings generated."
        ),
        @Metric(
            name = "token.count",
            type = Counter.TYPE,
            description = "Total number of tokens consumed by the embedding model."
        )
    }
)
public class Embeddings extends AbstractTask implements RunnableTask<Embeddings.Output> {

    private static final String DEFAULT_MODEL = "text-embedding-005";
    private static final int MAX_BATCH_SIZE = 250;
    private static final Pattern REGION_PATTERN = Pattern.compile("^[a-z]+(-[a-z]+)*[0-9]+$");

    @Schema(
        title = "Region",
        description = "Vertex AI regional endpoint (e.g., us-central1, europe-west1)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> region;

    @Schema(
        title = "Model ID",
        description = "Vertex AI embedding model identifier (e.g., text-embedding-005, text-multilingual-embedding-002)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> modelId = Property.ofValue(DEFAULT_MODEL);

    @Schema(
        title = "Text",
        description = "Single text string to generate an embedding for. Mutually exclusive with `texts`."
    )
    @PluginProperty(group = "main")
    private Property<String> text;

    @Schema(
        title = "Texts",
        description = "List of text strings to generate embeddings for in a batch (maximum 250 texts per request). Mutually exclusive with `text`."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> texts;

    @Schema(
        title = "Task type",
        description = """
            Intended downstream use case for the embedding vector:
            - `RETRIEVAL_QUERY`: Specifies the given text is a query in a search/retrieval setting.
            - `RETRIEVAL_DOCUMENT`: Specifies the given text is a document in a search/retrieval setting.
            - `SEMANTIC_SIMILARITY`: Specifies the given text will be used for STS (semantic textual similarity).
            - `CLASSIFICATION`: Specifies that the embeddings will be used for classification.
            - `CLUSTERING`: Specifies that the embeddings will be used for clustering.
            - `QUESTION_ANSWERING`: Specifies that the embeddings will be used for question answering.
            - `FACT_VERIFICATION`: Specifies that the embeddings will be used for fact verification.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<TaskType> taskType;

    @Schema(
        title = "Document title",
        description = "Optional title for the text, recommended when taskType is RETRIEVAL_DOCUMENT."
    )
    @PluginProperty(group = "advanced")
    private Property<String> title;

    @Schema(
        title = "Output dimensionality",
        description = "Optional reduced dimension size (e.g., 256, 512, 768) supported by modern embedding models."
    )
    @PluginProperty(group = "advanced")
    private Property<@Positive Integer> outputDimensionality;

    @Schema(
        title = "Auto truncate",
        description = "Whether to automatically truncate input text exceeding the model's token limit. Defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoTruncate = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rRegion = runContext.render(this.region).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Property 'region' is required"));

        if (!REGION_PATTERN.matcher(rRegion).matches()) {
            throw new IllegalArgumentException("Property 'region' must be a valid GCP region (e.g., 'us-central1', 'europe-west1'), got: '" + rRegion + "'");
        }

        var rProjectId = runContext.render(this.getProjectId()).as(String.class).orElse(null);

        var rModelId = runContext.render(this.modelId).as(String.class)
            .orElse(DEFAULT_MODEL);

        var rText = this.text != null ? runContext.render(this.text).as(String.class).orElse(null) : null;
        var rTexts = this.texts != null ? runContext.render(this.texts).asList(String.class) : null;

        var hasSingleText = rText != null && !rText.isBlank();
        var hasBatchTexts = rTexts != null && !rTexts.isEmpty();

        if (hasSingleText && hasBatchTexts) {
            throw new IllegalArgumentException("Properties 'text' and 'texts' are mutually exclusive. Please specify only one.");
        }
        if (!hasSingleText && !hasBatchTexts) {
            throw new IllegalArgumentException("Either 'text' or 'texts' must be provided and not empty.");
        }

        var inputTexts = hasSingleText ? List.of(rText) : rTexts;

        if (inputTexts.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(String.format(
                "Batch size of %d exceeds the maximum allowed batch size of %d instances per request. Please split your texts into batches of up to %d.",
                inputTexts.size(),
                MAX_BATCH_SIZE,
                MAX_BATCH_SIZE
            ));
        }

        var rTaskType = this.taskType != null
            ? runContext.render(this.taskType).as(TaskType.class).orElse(null)
            : null;

        var rTitle = this.title != null
            ? runContext.render(this.title).as(String.class).orElse(null)
            : null;

        var rOutputDimensionality = this.outputDimensionality != null
            ? runContext.render(this.outputDimensionality).as(Integer.class).orElse(null)
            : null;

        var rAutoTruncate = this.autoTruncate != null
            ? runContext.render(this.autoTruncate).as(Boolean.class).orElse(true)
            : true;

        var credentials = this.credentials(runContext);

        if (rProjectId == null) {
            if (credentials instanceof ServiceAccountCredentials sac) {
                rProjectId = sac.getProjectId();
            } else if (credentials.getQuotaProjectId() != null) {
                rProjectId = credentials.getQuotaProjectId();
            }
        }

        if (rProjectId == null) {
            throw new IllegalArgumentException("Property 'projectId' is required or must be configured in credentials");
        }

        var credentialsProvider = FixedCredentialsProvider.create(credentials);

        var settings = PredictionServiceSettings.newBuilder()
            .setEndpoint(rRegion + "-aiplatform.googleapis.com:443")
            .setCredentialsProvider(credentialsProvider)
            .build();

        var endpointName = EndpointName.ofProjectLocationPublisherModelName(
            rProjectId,
            rRegion,
            "google",
            rModelId
        );

        var instances = new ArrayList<Value>(inputTexts.size());
        for (var content : inputTexts) {
            var instanceStruct = Struct.newBuilder();
            instanceStruct.putFields("content", Value.newBuilder().setStringValue(content).build());

            if (rTaskType != null) {
                instanceStruct.putFields("task_type", Value.newBuilder().setStringValue(rTaskType.name()).build());
            }

            if (rTitle != null) {
                instanceStruct.putFields("title", Value.newBuilder().setStringValue(rTitle).build());
            }

            instances.add(Value.newBuilder().setStructValue(instanceStruct.build()).build());
        }

        Value parametersValue;
        if (rOutputDimensionality != null || !rAutoTruncate) {
            var paramsStruct = Struct.newBuilder();
            if (rOutputDimensionality != null) {
                paramsStruct.putFields("outputDimensionality", Value.newBuilder().setNumberValue(rOutputDimensionality).build());
            }
            if (!rAutoTruncate) {
                paramsStruct.putFields("autoTruncate", Value.newBuilder().setBoolValue(false).build());
            }
            parametersValue = Value.newBuilder().setStructValue(paramsStruct.build()).build();
        } else {
            parametersValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }

        var predictRequest = PredictRequest.newBuilder()
            .setEndpoint(endpointName.toString())
            .addAllInstances(instances)
            .setParameters(parametersValue)
            .build();

        runContext.logger().debug("Sending embeddings request to Vertex AI for {} instances with model '{}'", instances.size(), rModelId);

        var response = executePrediction(settings, predictRequest);

        var allEmbeddings = new ArrayList<List<Double>>(response.getPredictionsCount());
        var totalTokens = 0L;

        for (var prediction : response.getPredictionsList()) {
            if (!prediction.hasStructValue()) {
                continue;
            }
            var predStruct = prediction.getStructValue();
            var embeddingsField = predStruct.getFieldsOrDefault("embeddings", Value.getDefaultInstance());

            if (embeddingsField.hasStructValue()) {
                var embStruct = embeddingsField.getStructValue();
                var valuesField = embStruct.getFieldsOrDefault("values", Value.getDefaultInstance());

                if (valuesField.hasListValue()) {
                    var vector = valuesField.getListValue().getValuesList().stream()
                        .map(Value::getNumberValue)
                        .toList();
                    allEmbeddings.add(vector);
                }

                var statsField = embStruct.getFieldsOrDefault("statistics", Value.getDefaultInstance());
                if (statsField.hasStructValue()) {
                    var tokenCountField = statsField.getStructValue().getFieldsOrDefault("token_count", Value.getDefaultInstance());
                    if (tokenCountField.hasNumberValue()) {
                        totalTokens += (long) tokenCountField.getNumberValue();
                    }
                }
            }
        }

        if (allEmbeddings.size() != inputTexts.size()) {
            throw new IllegalStateException(String.format(
                "Vertex AI returned %d embeddings for %d input texts. Expected 1:1 correspondence.",
                allEmbeddings.size(),
                inputTexts.size()
            ));
        }

        runContext.metric(Counter.of("embeddings.count", allEmbeddings.size()));
        if (totalTokens > 0) {
            runContext.metric(Counter.of("token.count", totalTokens));
        }

        var singleEmbedding = (hasSingleText && !allEmbeddings.isEmpty()) ? allEmbeddings.getFirst() : null;
        var dimension = !allEmbeddings.isEmpty() ? allEmbeddings.getFirst().size() : 0;

        return Output.builder()
            .embeddings(allEmbeddings)
            .embedding(singleEmbedding)
            .count(allEmbeddings.size())
            .dimension(dimension)
            .build();
    }

    PredictResponse executePrediction(PredictionServiceSettings settings, PredictRequest predictRequest) throws Exception {
        try (var client = PredictionServiceClient.create(settings)) {
            return client.predict(predictRequest);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Generated embeddings",
            description = "List of vector embeddings for each input text."
        )
        private List<List<Double>> embeddings;

        @Schema(
            title = "Single embedding vector",
            description = "Convenience vector populated when a single `text` property is provided."
        )
        private List<Double> embedding;

        @Schema(
            title = "Count",
            description = "Total number of embedding vectors generated."
        )
        private Integer count;

        @Schema(
            title = "Dimension",
            description = "Dimensionality size of the returned vectors."
        )
        private Integer dimension;
    }
}
