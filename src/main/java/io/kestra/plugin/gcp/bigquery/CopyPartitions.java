package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobInfo;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy partitions between interval to another table "
)
@Plugin(
    examples = {
        @Example(
            code = {
                "projectId: my-project",
                "dataset: my-dataset",
                "table: my-table",
                "destinationTable: my-dest-table",
                "partitionType: DAY",
                "from: \"{{ now() | dateAdd(-30, 'DAYS') }}\"",
                "to: \"{{ now() | dateAdd(-7, 'DAYS') }}\""
            }
        )
    },
    metrics = {
        @Metric(name = "size", type = Counter.TYPE, description = "The number of partitions copied.")
    }
)
public class CopyPartitions extends AbstractPartition implements RunnableTask<CopyPartitions.Output>, AbstractJobInterface {
    protected String destinationTable;

    protected JobInfo.WriteDisposition writeDisposition;

    protected JobInfo.CreateDisposition createDisposition;

    protected Duration jobTimeout;

    protected Map<String, String> labels;

    @Builder.Default
    protected Boolean dryRun = false;

    @Override
    public CopyPartitions.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        List<String> partitionToCopy = this.listPartitions(runContext, connection, tableId);

        logger.debug("Copying partitions '{}'", partitionToCopy);
        runContext.metric(Counter.of("size", partitionToCopy.size()));

        Copy task = Copy.builder()
            .sourceTables(partitionToCopy
                .stream()
                .map(throwFunction(s -> {
                    TableId current = this.tableId(runContext, s);
                    List<String> source = new ArrayList<>();
                    if (current.getProject() != null) {
                        source.add(current.getProject());
                    }

                    source.add(current.getDataset());
                    source.add(current.getTable());

                    return String.join(".", source);
                }))
                .collect(Collectors.toList())
            )
            .destinationTable(this.destinationTable)
            .writeDisposition(this.writeDisposition)
            .createDisposition(this.createDisposition)
            .jobTimeout(this.jobTimeout)
            .labels(this.labels)
            .dryRun(this.dryRun)
            .location(this.location)
            .retryAuto(this.retryAuto)
            .retryReasons(this.retryReasons)
            .retryMessages(this.retryMessages)
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .build();

        Copy.Output run = task.run(runContext);

        return Output.of(tableId, partitionToCopy, run.getJobId());
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
            title = "Partitions copied"
        )
        private final List<String> partitions;

        @Schema(
            title = "The job id"
        )
        private String jobId;

        public static Output of(TableId table, List<String> partitions, String jobId) {
            return Output.builder()
                .projectId(table.getProject())
                .datasetId(table.getDataset())
                .table(table.getTable())
                .partitions(partitions)
                .build();
        }
    }
}
