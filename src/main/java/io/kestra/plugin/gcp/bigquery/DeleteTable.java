package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a table or a partition"
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a partition",
            full = true,
            code = """
                id: gcp_bq_delete_table
                namespace: company.name

                tasks:
                  - id: delete_table
                    type: io.kestra.plugin.gcp.bigquery.DeleteTable
                    projectId: my-project
                    dataset: my-dataset
                    table: my-table$20130908
                """
        )
    }
)
public class DeleteTable extends AbstractTable implements RunnableTask<DeleteTable.Output> {
    @Override
    public DeleteTable.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        logger.debug("Deleting table '{}'", tableId);

        boolean delete = connection.delete(tableId);

        if (!delete) {
            throw new Exception("Couldn't find table '" + tableId + "'");
        }

        return Output.of(tableId);
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The project's id"
        )
        private final String projectId;

        @Schema(
            title = "The dataset's id"
        )
        private final String datasetId;

        @Schema(
            title = "The table name"
        )
        private final String table;

        public static Output of(TableId table) {
            return Output.builder()
                .projectId(table.getProject())
                .datasetId(table.getDataset())
                .table(table.getTable())
                .build();
        }
    }
}
