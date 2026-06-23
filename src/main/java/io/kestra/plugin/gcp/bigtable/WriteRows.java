package io.kestra.plugin.gcp.bigtable;

import java.util.List;
import java.util.Map;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

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
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write rows to a Google Cloud Bigtable table.",
    description = "Writes one or more rows as a batch of mutations. Each row can set one or more cells " +
        "(`cells`) and/or delete one or more cells (`deleteCells`) within the configured column family."
)
@Plugin(
    examples = {
        @Example(
            title = "Write rows to Bigtable.",
            full = true,
            code = """
                id: bigtable_write_rows
                namespace: company.team

                inputs:
                  - id: table_id
                    type: STRING
                    defaults: events

                tasks:
                  - id: write
                    type: io.kestra.plugin.gcp.bigtable.WriteRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: "{{ inputs.table_id }}"
                    columnFamily: cf1
                    rows:
                      - rowKey: "row-001"
                        cells:
                          value: "42"

                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Written {{ outputs.write.rowCount }} rows to {{ inputs.table_id }}"
                """
        ),
        @Example(
            title = "Set and delete cells on the same row.",
            full = true,
            code = """
                id: bigtable_write_rows_with_delete
                namespace: company.team

                tasks:
                  - id: write
                    type: io.kestra.plugin.gcp.bigtable.WriteRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: events
                    columnFamily: cf1
                    rows:
                      - rowKey: "row-001"
                        cells:
                          status: "processed"
                        deleteCells:
                          - stale_flag
                """
        )
    },
    metrics = {
        @Metric(
            name = "rows.written",
            type = Counter.TYPE,
            unit = "rows",
            description = "Number of rows written."
        )
    }
)
public class WriteRows extends AbstractBigtable implements RunnableTask<WriteRows.Output> {

    @NotNull
    @Schema(
        title = "The Bigtable table ID to write to"
    )
    @PluginProperty(group = "destination")
    private Property<String> tableId;

    @Schema(
        title = "The column family to write cells into",
        description = "Can be overridden per-row by setting `columnFamily` on an individual row entry."
    )
    @PluginProperty(group = "destination")
    private Property<String> columnFamily;

    @NotNull
    @Schema(
        title = "The rows to write",
        description = "Each row has a row key, a map of column qualifier to cell value to set, and an " +
            "optional list of column qualifiers to delete."
    )
    @PluginProperty(group = "main")
    private Property<List<RowInput>> rows;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        String rDefaultFamily = runContext.render(this.columnFamily).as(String.class).orElse(null);
        List<RowInput> rRows = runContext.render(this.rows).asList(RowInput.class);

        logger.debug("Writing {} row(s) to Bigtable table '{}'", rRows.size(), rTableId);

        BulkMutation bulkMutation = BulkMutation.create(rTableId);

        for (RowInput rowInput : rRows) {
            String family = rowInput.getColumnFamily() != null ? rowInput.getColumnFamily() : rDefaultFamily;
            if (family == null) {
                throw new IllegalArgumentException(
                    "No columnFamily defined for row '" + rowInput.getRowKey() + "' and no default columnFamily set on the task."
                );
            }

            RowMutationEntry entry = RowMutationEntry.create(rowInput.getRowKey());

            if (rowInput.getCells() != null) {
                for (Map.Entry<String, String> cell : rowInput.getCells().entrySet()) {
                    entry = entry.setCell(family, cell.getKey(), cell.getValue());
                }
            }

            if (rowInput.getDeleteCells() != null) {
                for (String qualifier : rowInput.getDeleteCells()) {
                    entry = entry.deleteCells(family, qualifier);
                }
            }

            bulkMutation.add(entry);
        }

        try (BigtableDataClient client = this.dataClient(runContext)) {
            try {
                client.bulkMutateRows(bulkMutation);
            } catch (MutateRowsException e) {
                throw new RuntimeException("Failed to write one or more rows to Bigtable: " + e.getMessage(), e);
            }
        }

        runContext.metric(Counter.of("rows.written", rRows.size(), "table_id", rTableId));
        logger.info("Wrote {} row(s) to Bigtable table '{}'", rRows.size(), rTableId);

        return Output.builder()
            .rowCount((long) rRows.size())
            .build();
    }

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RowInput {
        @NotNull
        @Schema(title = "The row key")
        private String rowKey;

        @Schema(title = "The column family for this row", description = "Overrides the task-level `columnFamily` if set.")
        private String columnFamily;

        @Schema(title = "Map of column qualifier to cell value to set on this row")
        private Map<String, String> cells;

        @Schema(title = "Column qualifiers to delete on this row")
        private List<String> deleteCells;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows written")
        private final Long rowCount;
    }
}