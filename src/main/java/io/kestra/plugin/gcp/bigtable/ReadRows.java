package io.kestra.plugin.gcp.bigtable;

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read rows from a Google Cloud Bigtable table.",
    description = "Reads rows from a Bigtable table using an optional row key range, row key prefix, or cell filter. " +
        "Supports FETCH (return all rows in the output), FETCH_ONE (return only the first row), and STORE " +
        "(persist all rows to internal storage as ion, recommended for large result sets) fetch types."
)
@Plugin(
    examples = {
        @Example(
            title = "Read rows from Bigtable by row key prefix and store the result.",
            full = true,
            code = """
                id: bigtable_read_rows
                namespace: company.team

                tasks:
                  - id: read
                    type: io.kestra.plugin.gcp.bigtable.ReadRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: sensor-readings
                    rowKeyPrefix: "device-42#"
                    fetchType: STORE
                """
        ),
        @Example(
            title = "Read a single row by exact row key.",
            full = true,
            code = """
                id: bigtable_read_one_row
                namespace: company.team

                tasks:
                  - id: read
                    type: io.kestra.plugin.gcp.bigtable.ReadRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: sensor-readings
                    rowKeyStart: "device-42#001"
                    rowKeyEnd: "device-42#002"
                    fetchType: FETCH_ONE
                """
        )
    }
)
public class ReadRows extends AbstractBigtable implements RunnableTask<ReadRows.Output> {

    @NotNull
    @Schema(
        title = "The Bigtable table ID to read from"
    )
    @PluginProperty(group = "source")
    private Property<String> tableId;

    @Schema(
        title = "Inclusive start of the row key range to scan",
        description = "Mutually exclusive with `rowKeyPrefix`. If neither is set, the full table is scanned."
    )
    @PluginProperty(group = "source")
    private Property<String> rowKeyStart;

    @Schema(
        title = "Exclusive end of the row key range to scan",
        description = "Used together with `rowKeyStart`. Ignored if `rowKeyPrefix` is set."
    )
    @PluginProperty(group = "source")
    private Property<String> rowKeyEnd;

    @Schema(
        title = "Row key prefix to filter on",
        description = "Mutually exclusive with `rowKeyStart`/`rowKeyEnd`."
    )
    @PluginProperty(group = "source")
    private Property<String> rowKeyPrefix;

    @Schema(
        title = "Only return cells from this column family",
        description = "If not set, cells from all column families are returned."
    )
    @PluginProperty(group = "processing")
    private Property<String> columnFamily;

    @Schema(
        title = "Maximum number of rows to read",
        description = "If not set, all matching rows are read."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> limit;

    @Schema(
        title = "Data storage method",
        description = "FETCH_ONE outputs the first row, FETCH outputs all rows in the output variable, " +
            "STORE stores all rows to a file in internal storage and is recommended for large result sets."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.STORE);
        var rLimit = runContext.render(this.limit).as(Integer.class);

        try (BigtableDataClient client = this.dataClient(runContext)) {
            Query query = Query.create(rTableId);

            var rPrefix = runContext.render(this.rowKeyPrefix).as(String.class);
            var rStart = runContext.render(this.rowKeyStart).as(String.class);
            var rEnd = runContext.render(this.rowKeyEnd).as(String.class);

            if (rPrefix.isPresent()) {
                query = query.prefix(rPrefix.get());
            } else if (rStart.isPresent() || rEnd.isPresent()) {
                var range = com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange.unbounded();
                if (rStart.isPresent()) {
                    range = range.startClosed(rStart.get());
                }
                if (rEnd.isPresent()) {
                    range = range.endOpen(rEnd.get());
                }
                query = query.range(range);
            }

            var rFamily = runContext.render(this.columnFamily).as(String.class);
            if (rFamily.isPresent()) {
                query = query.filter(Filters.FILTERS.family().exactMatch(rFamily.get()));
            }

            if (rLimit.isPresent()) {
                query = query.limit(rLimit.get());
            }

            Output.OutputBuilder outputBuilder = Output.builder();
            long count = 0;
            Map<String, Object> firstRow = null;

            switch (rFetchType) {
                case FETCH_ONE -> {
                    for (Row row : client.readRows(query)) {
                        firstRow = rowToMap(row);
                        count = 1;
                        break;
                    }
                    outputBuilder.row(firstRow);
                }
                case FETCH -> {
                    List<Map<String, Object>> rows = new ArrayList<>();
                    int maxFetch = rLimit.orElse(5000);
                    if (!rLimit.isPresent()) {
                        query = query.limit(maxFetch);
                    }
                    for (Row row : client.readRows(query)) {
                        rows.add(rowToMap(row));
                    }
                    count = rows.size();
                    outputBuilder.rows(rows);
                }
                case NONE, STORE -> {
                    java.io.File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    try (OutputStream output = Files.newOutputStream(tempFile.toPath())) {
                        for (Row row : client.readRows(query)) {
                            FileSerde.write(output, rowToMap(row));
                            count++;
                        }
                    }
                    if (rFetchType == FetchType.STORE) {
                        outputBuilder.uri(runContext.storage().putFile(tempFile));
                    }
                }
            }

            return outputBuilder
                .rowCount(count)
                .build();
        }
    }

    private static Map<String, Object> rowToMap(Row row) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("rowKey", row.getKey().toStringUtf8());

        List<Map<String, Object>> cells = new ArrayList<>();
        for (RowCell cell : row.getCells()) {
            Map<String, Object> cellMap = new LinkedHashMap<>();
            cellMap.put("family", cell.getFamily());
            cellMap.put("qualifier", cell.getQualifier().toStringUtf8());
            cellMap.put("value", cell.getValue().toStringUtf8());
            cellMap.put("timestamp", cell.getTimestamp());
            cells.add(cellMap);
        }
        map.put("cells", cells);
        return map;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows read")
        private final Long rowCount;

        @Schema(title = "The first matching row (only set when fetchType is FETCH_ONE)")
        private final Map<String, Object> row;

        @Schema(title = "All matching rows (only set when fetchType is FETCH)")
        private final List<Map<String, Object>> rows;

        @Schema(title = "URI of the file storing all rows (only set when fetchType is STORE)")
        private final URI uri;
    }
}
