package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
    title = "Copy BigQuery partitions to another table",
    description = "Lists partitions in a source table over the given date range and copies them to a destination table using a BigQuery copy job. Supports create/write dispositions and dry run."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_bq_copy_partitions
                namespace: company.team

                tasks:
                  - id: copy_partitions
                    type: io.kestra.plugin.gcp.bigquery.CopyPartitions
                    projectId: my-project
                    dataset: my-dataset
                    table: my-table
                    destinationTable: my-dest-table
                    partitionType: DAY
                    from: "{{ now() | dateAdd(-30, 'DAYS') }}"
                    to: "{{ now() | dateAdd(-7, 'DAYS') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "size", type = Counter.TYPE, description = "The number of partitions copied.")
    }
)
public class CopyPartitions extends AbstractPartition implements RunnableTask<CopyPartitions.Output>, AbstractJobInterface {
    @Schema(
        title = "Destination table",
        description = "Target table receiving the copied partitions"
    )
    protected Property<String> destinationTable;

    @Schema(
        title = "Write disposition",
        description = "BigQuery write disposition applied to the copy job"
    )
    protected Property<JobInfo.WriteDisposition> writeDisposition;

    @Schema(
        title = "Create disposition",
        description = "BigQuery create disposition applied to the copy job"
    )
    protected Property<JobInfo.CreateDisposition> createDisposition;

    @Schema(
        title = "Job timeout",
        description = "Optional maximum duration for the copy job"
    )
    protected Property<Duration> jobTimeout;

    @Schema(
        title = "Job labels"
    )
    protected Property<Map<String, String>> labels;

    @Builder.Default
    @Schema(
        title = "Dry run",
        description = "If true, validates the job without executing"
    )
    protected Property<Boolean> dryRun = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Skip empty partitions",
        description = "If true, the task completes successfully without running a copy job when no partitions are found in the specified range. " +
            "If false (the default), the task will fail when there are no partitions to copy."
    )
    protected Property<Boolean> skipEmpty = Property.ofValue(false);

    @Override
    public CopyPartitions.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        List<String> partitionToCopy = this.listPartitions(runContext, connection, tableId);

        logger.debug("Copying partitions '{}'", partitionToCopy);
        runContext.metric(Counter.of("size", partitionToCopy.size()));

        if (partitionToCopy.isEmpty()) {
            boolean skip = runContext.render(this.skipEmpty).as(Boolean.class).orElse(false);
            if (skip) {
                logger.info("No partitions found in the specified range, skipping copy (skipEmpty=true)");
                return Output.of(tableId, partitionToCopy, null);
            }
        }

        Copy task = Copy.builder()
            .sourceTables(Property.ofValue(partitionToCopy
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
            ))
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
            title = "Project ID"
        )
        private final String projectId;

        @Schema(
            title = "Dataset ID"
        )
        private final String datasetId;

        @Schema(
            title = "Table name"
        )
        private final String table;

        @Schema(
            title = "Partitions copied"
        )
        private final List<String> partitions;

        @Schema(
            title = "Job ID"
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
