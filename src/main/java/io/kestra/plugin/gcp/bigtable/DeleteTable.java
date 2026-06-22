package io.kestra.plugin.gcp.bigtable;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;

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
    title = "Delete a Google Cloud Bigtable table.",
    description = "Permanently deletes a table and all of its data. This action cannot be undone."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a Bigtable table.",
            full = true,
            code = """
                id: bigtable_delete_table
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.gcp.bigtable.DeleteTable
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    tableId: sensor-readings
                """
        )
    }
)
public class DeleteTable extends AbstractBigtable implements RunnableTask<DeleteTable.Output> {

    @NotNull
    @Schema(
        title = "The Bigtable table ID to delete."
    )
    @PluginProperty(group = "main")
    private Property<String> tableId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rTableId = runContext.render(this.tableId).as(String.class).orElseThrow();

        try (BigtableTableAdminClient client = this.adminClient(runContext)) {
            client.deleteTable(rTableId);
        }

        return Output.builder()
            .tableId(rTableId)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The deleted table ID.")
        private final String tableId;
    }
}
