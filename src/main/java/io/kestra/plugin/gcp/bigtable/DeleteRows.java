package io.kestra.plugin.gcp.bigtable;

import java.util.List;

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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete rows from a Google Cloud Bigtable table", description = "Deletes rows either by an explicit list of row keys, or by a row key range "
        +
        "(`rowKeyStart`/`rowKeyEnd`) or prefix (`rowKeyPrefix`). At least one mode must be configured; if several are set, `rowKeys` takes precedence, then the prefix, then the range."
)
@Plugin(
    examples = {
        @Example(title = "Delete rows by exact row keys", full = true, code = """
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
            """),
        @Example(title = "Delete all rows matching a row key prefix", full = true, code = """
            id: bigtable_delete_rows_prefix
            namespace: company.team

            tasks:
              - id: delete
                type: io.kestra.plugin.gcp.bigtable.DeleteRows
                projectId: "{{ secret('GCP_PROJECT_ID') }}"
                instanceId: my-instance
                tableId: events
                rowKeyPrefix: "stale-device#"
            """)
    }
)
public class DeleteRows extends AbstractBigtable implements RunnableTask<DeleteRows.Output> {

    @NotNull
    @Schema(title = "The Bigtable table ID to delete rows from")
    @PluginProperty(group = "main")
    private Property<String> tableId;

    @Schema(title = "Exact row keys to delete", description = "Takes precedence over `rowKeyStart`/`rowKeyEnd` and `rowKeyPrefix` if several are set.")
    @PluginProperty(group = "source")
    private Property<List<String>> rowKeys;

    @Schema(title = "Inclusive start of the row key range to delete")
    @PluginProperty(group = "source")
    private Property<String> rowKeyStart;

    @Schema(title = "Exclusive end of the row key range to delete")
    @PluginProperty(group = "source")
    private Property<String> rowKeyEnd;

    @Schema(title = "Row key prefix to match rows for deletion", description = "Used when `rowKeys` is not set; takes precedence over `rowKeyStart`/`rowKeyEnd` if both are set.")
    @PluginProperty(group = "source")
    private Property<String> rowKeyPrefix;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        var rRowKeys = runContext.render(this.rowKeys).asList(String.class);
        var rPrefix = runContext.render(this.rowKeyPrefix).as(String.class);
        var rStart = runContext.render(this.rowKeyStart).as(String.class);
        var rEnd = runContext.render(this.rowKeyEnd).as(String.class);

        try (BigtableDataClient client = this.dataClient(runContext)) {
            long count;

            if (!rRowKeys.isEmpty()) {
                BulkMutation bulkMutation = BulkMutation.create(rTableId);
                long idx = 0;
                for (String key : rRowKeys) {
                    bulkMutation.add(RowMutationEntry.create(key).deleteRow());
                    idx++;
                    if (idx % 1000 == 0) {
                        client.bulkMutateRows(bulkMutation);
                        bulkMutation = BulkMutation.create(rTableId);
                    }
                }
                if (idx % 1000 != 0) {
                    client.bulkMutateRows(bulkMutation);
                }
                count = rRowKeys.size();
            } else if (rPrefix.isPresent() || rStart.isPresent() || rEnd.isPresent()) {
                Query query = Query.create(rTableId);
                if (rPrefix.isPresent()) {
                    query = query.prefix(rPrefix.get());
                } else {
                    var range = com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange.unbounded();
                    if (rStart.isPresent()) {
                        range = range.startClosed(rStart.get());
                    }
                    if (rEnd.isPresent()) {
                        range = range.endOpen(rEnd.get());
                    }
                    query = query.range(range);
                }

                BulkMutation bulkMutation = BulkMutation.create(rTableId);
                long matched = 0;
                for (Row row : client.readRows(query)) {
                    bulkMutation.add(RowMutationEntry.create(row.getKey()).deleteRow());
                    matched++;
                    if (matched % 1000 == 0) {
                        client.bulkMutateRows(bulkMutation);
                        bulkMutation = BulkMutation.create(rTableId);
                    }
                }
                if (matched % 1000 != 0) {
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
        @Schema(title = "Number of rows deleted")
        private final Long rowCount;
    }
}
