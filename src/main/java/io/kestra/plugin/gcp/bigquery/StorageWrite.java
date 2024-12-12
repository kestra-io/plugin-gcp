package io.kestra.plugin.gcp.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.primitives.Primitives;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_bq_storage_write
                namespace: company.team

                tasks:
                  - id: read_data
                    type: io.kestra.plugin.core.http.Download
                    uri: https://dummyjson.com/products/1

                  - id: storage_write
                    type: io.kestra.plugin.gcp.bigquery.StorageWrite
                    from: "{{ outputs.read_data.uri }}"
                    destinationTable: "my_project.my_dataset.my_table"
                    writeStreamType: DEFAULT
                """
        )
    },
    metrics = {
        @Metric(name = "rows", type = Counter.TYPE, description = "Rows count."),
        @Metric(name = "rows_count", type = Counter.TYPE, description = "Rows count reported by BigQuery, only on `PENDING` writeStreamType.")
    }
)
@Schema(
    title = "Load an kestra internal storage file on bigquery using " +
        "[BigQuery Storage API](https://cloud.google.com/bigquery/docs/write-api#write_to_a_stream_in_committed_mode)"
)
public class StorageWrite extends AbstractTask implements RunnableTask<StorageWrite.Output> {
    @Schema(
        title = "The fully-qualified URIs that point to source data"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The table where to load data",
        description = "The table must be created before."
    )
    @PluginProperty(dynamic = true)
    private String destinationTable;

    @NotNull
    @NotEmpty
    @Builder.Default
    @Schema(
        title = "The type of write stream to use"
    )
    @PluginProperty
    private final WriteStreamType writeStreamType = WriteStreamType.DEFAULT;

    @NotNull
    @Builder.Default
    @Schema(
        title = "The number of records to send on each query"
    )
    @PluginProperty
    protected final Integer bufferSize = 1000;

    @Schema(
        title = "The geographic location where the dataset should reside",
        description = "This property is experimental" +
            " and might be subject to change or removed.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset Location</a>"
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        TableId tableId = BigQueryService.tableId(runContext.render(this.destinationTable));

        if (tableId.getProject() == null || tableId.getDataset() == null || tableId.getTable() == null) {
            throw new Exception("Invalid destinationTable " + tableId);
        }

        TableName parentTable = TableName.of(tableId.getProject(), tableId.getDataset(), tableId.getTable());

        // reader
        URI from = new URI(runContext.render(this.from));

        try (
            BigQueryWriteClient connection = this.connection(runContext);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)), FileSerde.BUFFER_SIZE)
        ) {
            try (JsonStreamWriter writer = this.jsonStreamWriter(runContext, parentTable, connection).build()) {
                Integer count = FileSerde.readAll(inputStream)
                    .map(this::map)
                    .buffer(this.bufferSize)
                    .map(list -> {
                        JSONArray result = new JSONArray();
                        list.forEach(result::put);

                        return result;
                    })
                    .map(throwFunction(o -> {
                        try {
                            ApiFuture<AppendRowsResponse> future = writer.append(o);
                            AppendRowsResponse response = future.get();

                            return o.length();
                        } catch (ExecutionException e) {
                            // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
                            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
                            // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
                            throw new Exception("Failed to append records with error: " + e.getMessage(), e);
                        }
                    }))
                    .reduce(Integer::sum)
                    .block();

                Output.OutputBuilder builder = Output.builder()
                    .rows(count);

                if (this.writeStreamType == WriteStreamType.PENDING) {
                    logger.debug("Commit pending stream '{}'", writer.getStreamName());

                    // Commit the streams for PENDING
                    FinalizeWriteStreamResponse finalizeResponse = connection.finalizeWriteStream(writer.getStreamName());

                    builder.rowsCount(finalizeResponse.getRowCount());

                    BatchCommitWriteStreamsRequest commitRequest = BatchCommitWriteStreamsRequest
                        .newBuilder()
                        .setParent(parentTable.toString())
                        .addWriteStreams(writer.getStreamName())
                        .build();

                    BatchCommitWriteStreamsResponse commitResponse = connection.batchCommitWriteStreams(commitRequest);

                    if (!commitResponse.hasCommitTime()) {
                        // If the response does not have a commit time, it means the commit operation failed.
                        throw new Exception("Error on commit with error: " + commitResponse.getStreamErrorsList()
                            .stream()
                            .map(StorageError::getErrorMessage)
                            .collect(Collectors.joining("\n- ")));
                    } else {
                        builder.commitTime(Instant.ofEpochSecond(commitResponse.getCommitTime().getSeconds()));
                    }
                }

                Output output = builder
                    .build();

                String[] tags = tags(tableId);

                runContext.metric(Counter.of("rows", output.getRows(), tags));
                if (output.getRowsCount() != null) {
                    runContext.metric(Counter.of("rows_count", output.getRowsCount(), tags));
                }

                return output;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private JSONObject map(Object object) {
        if (!(object instanceof Map)) {
            throw new IllegalArgumentException("Unable to map with type '" + object.getClass().getName() + "'");
        }

        Map<String, Object> map = (Map<String, Object>) object;

        JSONObject record = new JSONObject();
        map.forEach((s, o) -> record.put(s, transform(o)));

        return record;
    }

    private Object transform(Object object) {
        if (object instanceof Map) {
            Map<?, ?> value = (Map<?, ?>) object;

            HashMap<Object, Object> map = value
                .entrySet()
                .stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                    e.getKey(),
                    transform(e.getValue())
                ))
                // https://bugs.openjdk.java.net/browse/JDK-8148463
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);

            return new JSONObject(map);
        } else if (object instanceof Collection) {
            Collection<?> value = (Collection<?>) object;
            return new JSONArray(value
                .stream()
                .map(this::transform)
                .collect(Collectors.toList())
            );
        } else if (object instanceof ZonedDateTime) {
            ZonedDateTime value = (ZonedDateTime) object;
            return value.toInstant().toEpochMilli() * 1000;
        } else if (object instanceof Instant) {
            Instant value = (Instant) object;
            return value.toEpochMilli() * 1000;
        } else if (object instanceof LocalDate) {
            LocalDate value = (LocalDate) object;
            return (int) value.toEpochDay();
        } else if (object instanceof LocalDateTime) {
            LocalDateTime value = (LocalDateTime) object;
            return CivilTimeEncoder.encodePacked64DatetimeMicros(org.threeten.bp.LocalDateTime.parse(value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        } else if (object instanceof LocalTime) {
            LocalTime value = (LocalTime) object;
            return CivilTimeEncoder.encodePacked64TimeMicros(org.threeten.bp.LocalTime.parse(value.format(DateTimeFormatter.ISO_LOCAL_TIME)));
        } else if (object instanceof OffsetTime) {
            OffsetTime value = (OffsetTime) object;
            return CivilTimeEncoder.encodePacked64TimeMicros(org.threeten.bp.LocalTime.parse(value.format(DateTimeFormatter.ISO_LOCAL_TIME)));
        } else if (object == null) {
            return null;
        } else if (object instanceof String) {
            return object;
        } else if (object.getClass().isPrimitive() || Primitives.isWrapperType(object.getClass())) {
            return object;
        } else if (object instanceof Enum) {
            return ((Enum<?>) object).name();
        } else {
            return object;
        }
    }

    private BigQueryWriteClient connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
            .newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .setQuotaProjectId(runContext.render(this.projectId).as(String.class).orElse(null))
            .build();

        return BigQueryWriteClient.create(bigQueryWriteSettings);
    }

    private String[] tags(TableId tableId) {
        return new String[]{
            "write_stream_type", this.writeStreamType.name(),
            "project_id", tableId.getProject(),
            "dataset", tableId.getDataset(),
        };
    }

    private JsonStreamWriter.Builder jsonStreamWriter(RunContext runContext, TableName parentTable, BigQueryWriteClient client) throws IllegalVariableEvaluationException, IOException {
        if (this.writeStreamType == WriteStreamType.DEFAULT) {
            // Write to the default stream: https://cloud.google.com/bigquery/docs/write-api#write_to_the_default_stream
            BigQuery bigQuery = AbstractBigquery.connection(
                runContext,
                this.credentials(runContext),
                runContext.render(this.projectId).as(String.class).orElse(null),
                this.location
            );

            TableId tableId = BigQueryService.tableId(runContext.render(this.destinationTable));

            Table table = bigQuery.getTable(tableId.getDataset(), tableId.getTable());

            if (table == null) {
                throw new IllegalArgumentException("No table '" + tableId + "' exists");
            }

            com.google.cloud.bigquery.Schema schema = table.getDefinition().getSchema();

            if (schema == null) {
                throw new IllegalArgumentException("No schema defined for table '" + tableId);
            }

            TableSchema tableSchema = BigQueryToBigQueryStorageSchemaConverter.convertTableSchema(schema);

            return JsonStreamWriter.newBuilder(parentTable.toString(), tableSchema);
        } else  {
            // Write to a stream in pending mode: https://cloud.google.com/bigquery/docs/write-api#write_to_a_stream_in_pending_mode
            // Write to a stream in committed mode : https://cloud.google.com/bigquery/docs/write-api#write_to_a_stream_in_committed_mode

            WriteStream stream = WriteStream
                .newBuilder()
                .setType(WriteStream.Type.valueOf(this.writeStreamType.name()))
                .build();

            CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest
                .newBuilder()
                .setParent(parentTable.toString())
                .setWriteStream(stream)
                .build();
            WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

            return JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema(), client);
        }
    }

    public enum WriteStreamType {
        DEFAULT,
        COMMITTED,
        PENDING
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Rows count"
        )
        private final Integer rows;

        @Schema(
            title = "Rows count reported by BigQuery, only on `PENDING` writeStreamType"
        )
        private final Long rowsCount;

        @Schema(
            title = "Commit time reported by BigQuery, only on `PENDING` writeStreamType"
        )
        private final Instant commitTime;
    }
}
