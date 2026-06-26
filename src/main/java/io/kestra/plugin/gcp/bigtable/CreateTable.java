package io.kestra.plugin.gcp.bigtable;

import java.util.List;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;

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
    title = "Create a Google Cloud Bigtable table",
    description = "Creates a table with the given column families. If the table already exists, the task fails " +
        "unless the underlying client treats creation as idempotent for the given table ID."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a Bigtable table with two column families",
            full = true,
            code = """
                id: bigtable_create_table
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.gcp.bigtable.CreateTable
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: sensor-readings
                    columnFamilies:
                      - cf1
                      - cf2
                """
        )
    }
)
public class CreateTable extends AbstractBigtable implements RunnableTask<CreateTable.Output> {

    @NotNull
    @Schema(
        title = "The Bigtable table ID to create"
    )
    @PluginProperty(group = "main")
    private Property<String> tableId;

    @Schema(
        title = "Column family names to create on the table",
        description = "If empty, the table is created with no column families."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> columnFamilies;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();
        List<String> rFamilies = runContext.render(this.columnFamilies).asList(String.class);

        try (BigtableTableAdminClient client = this.adminClient(runContext)) {
            CreateTableRequest request = CreateTableRequest.of(rTableId);
            for (String family : rFamilies) {
                request = request.addFamily(family);
            }
            client.createTable(request);
        }

        return Output.builder()
            .tableId(rTableId)
            .columnFamilyCount((long) rFamilies.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The created table ID")
        private final String tableId;

        @Schema(title = "Number of column families created")
        private final Long columnFamilyCount;
    }
}
