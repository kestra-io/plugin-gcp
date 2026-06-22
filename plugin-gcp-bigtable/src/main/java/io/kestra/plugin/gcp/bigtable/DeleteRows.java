package io.kestra.plugin.gcp.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete rows from a Google Cloud Bigtable table.",
    description = "Deletes rows either by an explicit list of row keys, or by a row key range " +
        "(`rowKeyStart`/`rowKeyEnd`) or prefix (`rowKeyPrefix`). Exactly one mode must be configured."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete rows by exact row keys.",
            full = true,
            code = """
                id: bigtable_delete_rows
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.gcp.bigtable.DeleteRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: events
                    rowKeys:
                      - row-001
                      - row-002
                """
        ),
        @Example(
            title = "Delete all rows matching a row key prefix.",
            full = true,
            code = """
                id: bigtable_delete_rows_prefix
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.gcp.bigtable.DeleteRows
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: events
                    rowKeyPrefix: "stale-device#"
                """
        )
    }
)
public class DeleteRows extends AbstractBigtable implements RunnableTask<DeleteRows.Output> {

    @Schema(
        title = "The Bigtable table ID to delete rows from."
    )
    @PluginProperty(dynamic = false)
    private Property<String> tableId;

    @Schema(
        title = "Exact row keys to delete.",
        description = "Mutually exclusive with `rowKeyStart`/`rowKeyEnd` and `rowKeyPrefix`."
    )
    private Property<List<String>> rowKeys;

    @Schema(
        title = "Inclusive start of the row key range to delete."
    )
    private Property<String> rowKeyStart;

    @Schema(
        title = "Exclusive end of the row key range to delete."
    )
    private Property<String> rowKeyEnd;

    @Schema(
        title = "Row key prefix to match rows for deletion.",
        description = "Mutually exclusive with `rowKeyStart`/`rowKeyEnd` and `rowKeys`."
    )
    private Property<String> rowKeyPrefix;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        var renderedRowKeys = runContext.render(this.rowKeys).asList(String.class);
        var prefix = runContext.render(this.rowKeyPrefix).as(String.class);
        var start = runContext.render(this.rowKeyStart).as(String.class);
        var end = runContext.render(this.rowKeyEnd).as(String.class);

        try (BigtableDataClient client = this.dataClient(runContext)) {
            long count;

            if (!renderedRowKeys.isEmpty()) {
                BulkMutation bulkMutation = BulkMutation.create(renderedTableId);
                for (String key : renderedRowKeys) {
                    bulkMutation.add(RowMutationEntry.create(key).deleteRow());
                }
                client.bulkMutateRows(bulkMutation);
                count = renderedRowKeys.size();
            } else if (prefix.isPresent() || start.isPresent() || end.isPresent()) {
                Query query = Query.create(renderedTableId);
                if (prefix.isPresent()) {
                    query = query.prefix(prefix.get());
                } else {
                    var range = com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange.unbounded();
                    if (start.isPresent()) {
                        range = range.startClosed(start.get());
                    }
                    if (end.isPresent()) {
                        range = range.endOpen(end.get());
                    }
                    query = query.range(range);
                }
                BulkMutation bulkMutation = BulkMutation.create(renderedTableId);
                long matched = 0;
                for (Row row : client.readRows(query)) {
                    bulkMutation.add(RowMutationEntry.create(row.getKey()).deleteRow());
                    matched++;
                }
                if (matched > 0) {
                    client.bulkMutateRows(bulkMutation);
                }
                count = matched;
            } else {
                throw new IllegalArgumentException(
                    "One of `rowKeys`, `rowKeyPrefix`, or `rowKeyStart`/`rowKeyEnd` must be set."
                );
            }

            return Output.builder().rowCount(count).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows deleted.")
        private final Long rowCount;
    }
}
