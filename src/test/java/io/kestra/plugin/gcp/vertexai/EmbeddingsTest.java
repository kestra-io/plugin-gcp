package io.kestra.plugin.gcp.vertexai;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.PredictRequest;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.vertexai.models.TaskType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class EmbeddingsTest {

    @Inject
    private RunContextFactory runContextFactory;

    private GoogleCredentials mockCredentials() {
        return mock(GoogleCredentials.class);
    }

    private PredictResponse createMockResponse(List<List<Double>> vectors, long tokenCount) {
        var responseBuilder = PredictResponse.newBuilder();
        for (var vector : vectors) {
            var listValue = ListValue.newBuilder();
            for (var val : vector) {
                listValue.addValues(Value.newBuilder().setNumberValue(val).build());
            }

            var embStruct = Struct.newBuilder()
                .putFields("values", Value.newBuilder().setListValue(listValue.build()).build())
                .putFields("statistics", Value.newBuilder().setStructValue(
                    Struct.newBuilder()
                        .putFields("token_count", Value.newBuilder().setNumberValue(tokenCount).build())
                        .putFields("truncated", Value.newBuilder().setBoolValue(false).build())
                        .build()
                ).build());

            var predStruct = Struct.newBuilder()
                .putFields("embeddings", Value.newBuilder().setStructValue(embStruct.build()).build());

            responseBuilder.addPredictions(Value.newBuilder().setStructValue(predStruct.build()).build());
        }
        return responseBuilder.build();
    }

    @Test
    void shouldGenerateSingleEmbedding() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_single_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("us-central1"))
            .modelId(Property.ofValue("text-embedding-005"))
            .text(Property.ofValue("How to configure GCS lifecycle rules?"))
            .taskType(Property.ofValue(TaskType.RETRIEVAL_QUERY))
            .outputDimensionality(Property.ofValue(768))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());

        var mockVector = List.of(0.0123, -0.0456, 0.0789);
        var mockResponse = createMockResponse(List.of(mockVector), 8L);
        doReturn(mockResponse).when(task).executePrediction(any(PredictionServiceSettings.class), any(PredictRequest.class));

        var runContext = runContextFactory.of(Map.of());
        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCount(), is(1));
        assertThat(output.getDimension(), is(3));
        assertThat(output.getEmbedding(), notNullValue());
        assertThat(output.getEmbedding(), is(mockVector));
        assertThat(output.getEmbeddings(), hasSize(1));
        assertThat(output.getEmbeddings().getFirst(), is(mockVector));

        var reqCaptor = ArgumentCaptor.forClass(PredictRequest.class);
        var settingsCaptor = ArgumentCaptor.forClass(PredictionServiceSettings.class);
        verify(task).executePrediction(settingsCaptor.capture(), reqCaptor.capture());

        assertThat(settingsCaptor.getValue().getEndpoint(), is("us-central1-aiplatform.googleapis.com:443"));
        var capturedReq = reqCaptor.getValue();
        assertThat(capturedReq.getEndpoint(), containsString("publishers/google/models/text-embedding-005"));
        assertThat(capturedReq.getInstancesCount(), is(1));

        var instanceStruct = capturedReq.getInstances(0).getStructValue();
        assertThat(instanceStruct.getFieldsOrThrow("content").getStringValue(), is("How to configure GCS lifecycle rules?"));
        assertThat(instanceStruct.getFieldsOrThrow("task_type").getStringValue(), is("RETRIEVAL_QUERY"));

        var paramsStruct = capturedReq.getParameters().getStructValue();
        assertThat(paramsStruct.getFieldsOrThrow("outputDimensionality").getNumberValue(), is(768.0));
    }

    @Test
    void shouldGenerateBatchEmbeddings() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_batch_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("europe-west1"))
            .modelId(Property.ofValue("text-multilingual-embedding-002"))
            .texts(Property.ofValue(List.of(
                "Cloud Storage is an object store.",
                "क्लाउड स्टोरेज एक स्केलेबल ऑब्जेक्ट स्टोरेज सेवा है।"
            )))
            .taskType(Property.ofValue(TaskType.RETRIEVAL_DOCUMENT))
            .title(Property.ofValue("Storage Overview"))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());

        var vec1 = List.of(0.1, 0.2, 0.3);
        var vec2 = List.of(0.4, 0.5, 0.6);
        var mockResponse = createMockResponse(List.of(vec1, vec2), 15L);
        doReturn(mockResponse).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of());
        var output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getCount(), is(2));
        assertThat(output.getDimension(), is(3));
        assertThat(output.getEmbedding(), nullValue());
        assertThat(output.getEmbeddings(), hasSize(2));
        assertThat(output.getEmbeddings().getFirst(), is(vec1));
        assertThat(output.getEmbeddings().get(1), is(vec2));

        var reqCaptor = ArgumentCaptor.forClass(PredictRequest.class);
        verify(task).executePrediction(any(), reqCaptor.capture());

        var capturedReq = reqCaptor.getValue();
        assertThat(capturedReq.getInstancesCount(), is(2));
        var inst1 = capturedReq.getInstances(0).getStructValue();
        assertThat(inst1.getFieldsOrThrow("content").getStringValue(), is("Cloud Storage is an object store."));
        assertThat(inst1.getFieldsOrThrow("task_type").getStringValue(), is("RETRIEVAL_DOCUMENT"));
        assertThat(inst1.getFieldsOrThrow("title").getStringValue(), is("Storage Overview"));

        var inst2 = capturedReq.getInstances(1).getStructValue();
        assertThat(inst2.getFieldsOrThrow("content").getStringValue(), is("क्लाउड स्टोरेज एक स्केलेबल ऑब्जेक्ट स्टोरेज सेवा है।"));
        assertThat(inst2.getFieldsOrThrow("task_type").getStringValue(), is("RETRIEVAL_DOCUMENT"));
        assertThat(inst2.getFieldsOrThrow("title").getStringValue(), is("Storage Overview"));
    }

    @Test
    void shouldFailWhenBothTextAndTextsProvided() {
        var task = Embeddings.builder()
            .id("embed_conflict_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .region(Property.ofValue("us-central1"))
            .text(Property.ofValue("Single text"))
            .texts(Property.ofValue(List.of("Batch text 1", "Batch text 2")))
            .build();

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("mutually exclusive"));
    }

    @Test
    void shouldFailWhenNeitherTextNorTextsProvided() {
        var task = Embeddings.builder()
            .id("embed_empty_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .region(Property.ofValue("us-central1"))
            .build();

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Either 'text' or 'texts' must be provided"));
    }

    @Test
    void shouldFailWhenTextIsBlank() {
        var task = Embeddings.builder()
            .id("embed_blank_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .region(Property.ofValue("us-central1"))
            .text(Property.ofValue("   "))
            .build();

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Either 'text' or 'texts' must be provided"));
    }

    @Test
    void shouldRenderPropertiesWithTemplateDelimitersWithoutDoubleEvaluation() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_templated_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofExpression("{{ project }}"))
            .region(Property.ofExpression("{{ loc }}"))
            .text(Property.ofExpression("{{ query_text }}"))
            .taskType(Property.ofValue(TaskType.SEMANTIC_SIMILARITY))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());

        var mockResponse = createMockResponse(List.of(List.of(0.12, 0.34)), 4L);
        doReturn(mockResponse).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of(
            "project", "templated-project-id",
            "loc", "us-east4",
            "query_text", "Literal search content {{ with_delimiters }}"
        ));

        var output = task.run(runContext);
        assertThat(output.getCount(), is(1));

        var reqCaptor = ArgumentCaptor.forClass(PredictRequest.class);
        verify(task).executePrediction(any(), reqCaptor.capture());
        var capturedReq = reqCaptor.getValue();
        assertThat(capturedReq.getEndpoint(), containsString("projects/templated-project-id/locations/us-east4"));
        assertThat(capturedReq.getInstances(0).getStructValue().getFieldsOrThrow("content").getStringValue(),
            is("Literal search content {{ with_delimiters }}"));
    }

    @Test
    void shouldFailWhenPredictionsCountDoesNotMatchInputCount() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_empty_resp_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("us-central1"))
            .text(Property.ofValue("Some text"))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());
        doReturn(PredictResponse.getDefaultInstance()).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Expected 1:1 correspondence"));
    }

    @Test
    void shouldConfigureAutoTruncateFalse() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_no_trunc_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("us-central1"))
            .text(Property.ofValue("Some text"))
            .autoTruncate(Property.ofValue(false))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());
        var mockResponse = createMockResponse(List.of(List.of(0.1)), 1L);
        doReturn(mockResponse).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of());
        task.run(runContext);

        var reqCaptor = ArgumentCaptor.forClass(PredictRequest.class);
        verify(task).executePrediction(any(), reqCaptor.capture());
        var params = reqCaptor.getValue().getParameters().getStructValue();
        assertThat(params.getFieldsOrThrow("autoTruncate").getBoolValue(), is(false));
    }

    @Test
    void shouldFailWhenProjectIdCannotBeResolved() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_no_proj_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .region(Property.ofValue("us-central1"))
            .text(Property.ofValue("Some text"))
            .build());

        doReturn(mock(GoogleCredentials.class)).when(task).credentials(any());

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Property 'projectId' is required"));
    }

    @Test
    void shouldExecuteSingleQueryEmbeddingFlow() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_query_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("my-gcp-project"))
            .region(Property.ofValue("us-central1"))
            .modelId(Property.ofValue("text-embedding-005"))
            .text(Property.ofValue("How to configure GCS lifecycle policies?"))
            .taskType(Property.ofValue(TaskType.RETRIEVAL_QUERY))
            .outputDimensionality(Property.ofValue(768))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());
        var mockResponse = createMockResponse(List.of(List.of(0.05, -0.12, 0.44)), 6L);
        doReturn(mockResponse).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of());
        var output = task.run(runContext);

        assertThat(output.getCount(), is(1));
        assertThat(output.getDimension(), is(3));
        assertThat(output.getEmbedding(), notNullValue());
        assertThat(output.getEmbedding(), contains(0.05, -0.12, 0.44));
    }

    @Test
    void shouldExecuteBatchChunksEmbeddingFlow() throws Exception {
        var task = spy(Embeddings.builder()
            .id("embed_chunks_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("my-gcp-project"))
            .region(Property.ofValue("us-central1"))
            .modelId(Property.ofValue("text-embedding-005"))
            .taskType(Property.ofValue(TaskType.RETRIEVAL_DOCUMENT))
            .title(Property.ofValue("GCS Architecture"))
            .texts(Property.ofValue(List.of(
                "Cloud Storage is an object storage service.",
                "Lifecycle rules automate transitions between storage classes."
            )))
            .build());

        doReturn(mockCredentials()).when(task).credentials(any());
        var mockResponse = createMockResponse(List.of(List.of(0.1, 0.2), List.of(0.3, 0.4)), 18L);
        doReturn(mockResponse).when(task).executePrediction(any(), any());

        var runContext = runContextFactory.of(Map.of());
        var output = task.run(runContext);

        assertThat(output.getCount(), is(2));
        assertThat(output.getDimension(), is(2));
        assertThat(output.getEmbedding(), nullValue());
        assertThat(output.getEmbeddings(), hasSize(2));
        assertThat(output.getEmbeddings().getFirst(), contains(0.1, 0.2));
        assertThat(output.getEmbeddings().get(1), contains(0.3, 0.4));
    }

    @Test
    void shouldFailWhenRegionIsInvalid() {
        var task = spy(Embeddings.builder()
            .id("embed_invalid_region_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("us-central1.evil.com"))
            .text(Property.ofValue("Some text"))
            .build());

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Property 'region' must be a valid GCP region"));
    }

    @Test
    void shouldFailWhenBatchSizeExceedsLimit() {
        var oversizedList = java.util.Collections.nCopies(251, "Sample chunk");
        var task = spy(Embeddings.builder()
            .id("embed_oversized_" + IdUtils.create())
            .type(Embeddings.class.getName())
            .projectId(Property.ofValue("test-project"))
            .region(Property.ofValue("us-central1"))
            .texts(Property.ofValue(oversizedList))
            .build());

        var runContext = runContextFactory.of(Map.of());
        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("exceeds the maximum allowed batch size of 250"));
    }
}
