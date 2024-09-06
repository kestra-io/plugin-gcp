package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.List;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete partitions between interval"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_bq_delete_partitions
                namespace: company.team

                tasks:
                  - id: delete_partitions
                    type: io.kestra.plugin.gcp.bigquery.DeletePartitions
                    projectId: my-project
                    dataset: my-dataset
                    table: my-table
                    partitionType: DAY
                    from: "{{ now() | dateAdd(-30, 'DAYS') }}"
                    to: "{{ now() | dateAdd(-7, 'DAYS') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "size", type = Counter.TYPE, description = "The number of partitions deleted.")
    }
)
public class DeletePartitions extends AbstractPartition implements RunnableTask<DeletePartitions.Output> {
    @Override
    public DeletePartitions.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        List<String> partitionsToDelete = this.listPartitions(runContext, connection, tableId);

        logger.debug("Deleting partitions '{}'", partitionsToDelete);
        runContext.metric(Counter.of("size", partitionsToDelete.size()));

        partitionsToDelete
            .parallelStream()
            .forEach(throwConsumer(s -> {
                TableId currentPartition = this.tableId(runContext, s);
                boolean delete = connection.delete(currentPartition);

                if (!delete) {
                    throw new Exception("Couldn't find partition '" + tableId + "$" + s + "'");
                }
            }));

        return Output.of(tableId, partitionsToDelete);
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

        @Schema(
            title = "Partitions deleted"
        )
        private final List<String> partitions;

        public static Output of(TableId table, List<String> partitions) {
            return Output.builder()
                .projectId(table.getProject())
                .datasetId(table.getDataset())
                .table(table.getTable())
                .partitions(partitions)
                .build();
        }
    }
}
