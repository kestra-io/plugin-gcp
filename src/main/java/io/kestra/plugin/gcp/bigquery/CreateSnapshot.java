package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.bigquery.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
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
@Plugin(
    examples = {
        @Example(
            title = "Create a table snapshot with 7-day automatic expiration",
            full = true,
            code = """
                id: gcp_bq_create_snapshot
                namespace: company.team

                tasks:
                  - id: create_snapshot
                    type: io.kestra.plugin.gcp.bigquery.CreateSnapshot
                    projectId: my-project
                    sourceDataset: analytics_prod
                    sourceTable: customer_orders
                    destinationDataset: analytics_snapshots
                    snapshotTable: "customer_orders_snap_{{ execution.startDate | date('yyyyMMdd') }}"
                    expirationDuration: P7D
                    description: "Daily automated pre-ETL snapshot"
                """
        ),
        @Example(
            title = "Capture a historical point-in-time snapshot using BigQuery Time Travel",
            full = true,
            code = """
                id: gcp_bq_historical_snapshot
                namespace: company.team

                tasks:
                  - id: historical_snapshot
                    type: io.kestra.plugin.gcp.bigquery.CreateSnapshot
                    projectId: my-project
                    sourceDataset: finance
                    sourceTable: ledger
                    snapshotTable: ledger_audit_eod
                    snapshotTimestamp: "{{ execution.startDate | dateAdd(-1, 'DAYS') }}"
                    expirationDuration: P30D
                """
        )
    },
    metrics = {
        @Metric(name = "num.child.jobs", type = Counter.TYPE, description = "The number of child jobs executed."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time it took for the job to run.")
    }
)
@Schema(
    title = "Create a BigQuery table snapshot",
    description = """
        Creates an instantaneous, zero-cost point-in-time table snapshot. Table snapshots charge $0 for unmodified bytes and support automatic TTL expiration. Accepts snapshotTimestamp for BigQuery Time Travel historical capture.
        """
)
public class CreateSnapshot extends AbstractBigquery implements RunnableTask<CreateSnapshot.Output> {
    @Schema(
        title = "Source dataset",
        description = """
            Dataset ID containing the base table to snapshot
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sourceDataset;

    @Schema(
        title = "Source table",
        description = """
            Name of the base table to snapshot
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> sourceTable;

    @Schema(
        title = "Source project ID",
        description = """
            GCP project ID containing the source table; defaults to projectId
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> sourceProject;

    @Schema(
        title = "Destination dataset",
        description = """
            Target dataset where the snapshot will be created; defaults to sourceDataset
            """
    )
    @PluginProperty(group = "main")
    private Property<String> destinationDataset;

    @Schema(
        title = "Snapshot table",
        description = """
            Name of the snapshot table to create
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> snapshotTable;

    @Schema(
        title = "Destination project ID",
        description = """
            GCP project ID for the snapshot table; defaults to projectId
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> destinationProject;

    @Schema(
        title = "Expiration duration",
        description = """
            TTL duration from now until the snapshot table automatically expires (e.g., P7D, P30D); if unset, the snapshot persists indefinitely
            """
    )
    @PluginProperty(group = "execution")
    private Property<Duration> expirationDuration;

    @Schema(
        title = "Snapshot timestamp",
        description = """
            Point-in-time timestamp (BigQuery Time Travel) to capture historical table state (within the 7-day time travel window)
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Instant> snapshotTimestamp;

    @Schema(
        title = "Snapshot description",
        description = """
            Friendly description for the snapshot table
            """
    )
    @PluginProperty(dynamic = true, group = "advanced")
    protected String description;

    @Schema(
        title = "Job timeout",
        description = """
            Optional max duration for the snapshot job; BigQuery may terminate the job when exceeded
            """
    )
    @PluginProperty(group = "execution")
    private Property<Duration> jobTimeout;

    @Schema(
        title = "Job labels",
        description = """
            Labels to apply to the BigQuery copy job
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> labels;

    @Builder.Default
    @Schema(
        title = "Dry run",
        description = """
            If true, validates the snapshot job without creating the table
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> dryRun = Property.ofValue(false);

    @Override
    public CreateSnapshot.Output run(RunContext runContext) throws Exception {
        var connection = this.connection(runContext);
        var logger = runContext.logger();

        var jobConfiguration = this.jobConfiguration(runContext);
        var destinationTableId = jobConfiguration.getDestinationTable();

        logger.debug("Creating snapshot of {} to {}", jobConfiguration.getSourceTables(), destinationTableId);

        var isDryRun = runContext.render(this.dryRun).as(Boolean.class).orElse(false);

        var snapshotJob = this.waitForJob(
            logger,
            () -> BigQueryService.createOrAdoptJob(
                connection,
                JobInfo.newBuilder(jobConfiguration)
                    .setJobId(BigQueryService.jobId(runContext, this))
                    .build(),
                logger
            ),
            isDryRun,
            runContext,
            connection
        );

        if (snapshotJob.getStatistics() instanceof JobStatistics.CopyStatistics copyJobStatistics) {
            this.metrics(runContext, copyJobStatistics, snapshotJob);
        }

        var output = Output.builder()
            .projectId(destinationTableId.getProject())
            .datasetId(destinationTableId.getDataset())
            .tableId(destinationTableId.getTable())
            .jobId(snapshotJob.getJobId() != null ? snapshotJob.getJobId().getJob() : null);

        if (Boolean.TRUE.equals(isDryRun)) {
            return output.build();
        }

        var table = connection.getTable(destinationTableId);
        if (table != null) {
            Table.Builder updateBuilder = null;

            if (this.description != null) {
                var rDescription = runContext.render(this.description);
                if (rDescription != null) {
                    updateBuilder = table.toBuilder().setDescription(rDescription);
                }
            }

            if (this.expirationDuration != null && table.getExpirationTime() == null) {
                var duration = runContext.render(this.expirationDuration).as(Duration.class).orElse(null);
                if (duration != null) {
                    if (updateBuilder == null) {
                        updateBuilder = table.toBuilder();
                    }
                    updateBuilder.setExpirationTime(Instant.now().plus(duration).toEpochMilli());
                }
            }

            if (updateBuilder != null) {
                table = connection.update(updateBuilder.build());
            }

            var snapshotTime = (Instant) null;
            if (table.getDefinition() instanceof SnapshotTableDefinition snapshotDefinition && snapshotDefinition.getSnapshotTime() != null) {
                try {
                    snapshotTime = Instant.parse(snapshotDefinition.getSnapshotTime());
                } catch (Exception ignored) {
                    // fallback to creation time
                }
            }
            if (snapshotTime == null && table.getCreationTime() != null) {
                snapshotTime = Instant.ofEpochMilli(table.getCreationTime());
            }

            output
                .projectId(table.getTableId().getProject() != null ? table.getTableId().getProject() : destinationTableId.getProject())
                .datasetId(table.getTableId().getDataset())
                .tableId(table.getTableId().getTable())
                .snapshotTime(snapshotTime)
                .expirationTime(table.getExpirationTime() != null ? Instant.ofEpochMilli(table.getExpirationTime()) : null)
                .totalRows(table.getNumRows() != null ? table.getNumRows().longValue() : null)
                .totalBytes(table.getNumBytes());
        }

        return output.build();
    }

    protected CopyJobConfiguration jobConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        var defaultProject = this.projectId != null ? runContext.render(this.projectId).as(String.class).orElse(null) : null;
        var srcProject = this.sourceProject != null ? runContext.render(this.sourceProject).as(String.class).orElse(defaultProject) : defaultProject;
        var destProject = this.destinationProject != null ? runContext.render(this.destinationProject).as(String.class).orElse(defaultProject) : defaultProject;

        var srcDataset = runContext.render(this.sourceDataset).as(String.class).orElseThrow(() -> new IllegalArgumentException("sourceDataset cannot be empty"));
        var srcTable = runContext.render(this.sourceTable).as(String.class).orElseThrow(() -> new IllegalArgumentException("sourceTable cannot be empty"));
        var destDataset = this.destinationDataset != null ? runContext.render(this.destinationDataset).as(String.class).orElse(srcDataset) : srcDataset;
        var snapTable = runContext.render(this.snapshotTable).as(String.class).orElseThrow(() -> new IllegalArgumentException("snapshotTable cannot be empty"));

        var sourceTableWithDecorator = srcTable;
        if (this.snapshotTimestamp != null) {
            var timestamp = runContext.render(this.snapshotTimestamp).as(Instant.class).orElse(null);
            if (timestamp != null) {
                sourceTableWithDecorator = srcTable + "@" + timestamp.toEpochMilli();
            }
        }

        var sourceTableId = srcProject != null
            ? TableId.of(srcProject, srcDataset, sourceTableWithDecorator)
            : TableId.of(srcDataset, sourceTableWithDecorator);

        var destinationTableId = destProject != null
            ? TableId.of(destProject, destDataset, snapTable)
            : TableId.of(destDataset, snapTable);

        var builder = CopyJobConfiguration.newBuilder(destinationTableId, sourceTableId)
            .setOperationType("SNAPSHOT");

        if (this.expirationDuration != null) {
            var duration = runContext.render(this.expirationDuration).as(Duration.class).orElse(null);
            if (duration != null) {
                builder.setDestinationExpirationTime(Instant.now().plus(duration).toString());
            }
        }

        if (this.jobTimeout != null) {
            var timeout = runContext.render(this.jobTimeout).as(Duration.class).orElse(null);
            if (timeout != null) {
                builder.setJobTimeoutMs(timeout.toMillis());
            }
        }

        var finalLabels = new HashMap<>(BigQueryService.labels(runContext));
        if (this.labels != null) {
            var rLabels = runContext.render(this.labels).asMap(String.class, String.class);
            if (!rLabels.isEmpty()) {
                finalLabels.putAll(rLabels);
            }
        }
        builder.setLabels(finalLabels);

        return builder.build();
    }

    private String[] tags(JobStatistics.CopyStatistics stats, Job queryJob) {
        var tags = new ArrayList<String>();
        if (queryJob.getJobId() != null) {
            if (queryJob.getJobId().getProject() != null) {
                tags.add("project_id");
                tags.add(queryJob.getJobId().getProject());
            }
            if (queryJob.getJobId().getLocation() != null) {
                tags.add("location");
                tags.add(queryJob.getJobId().getLocation());
            }
        }
        return tags.toArray(new String[0]);
    }

    private void metrics(RunContext runContext, JobStatistics.CopyStatistics stats, Job queryJob) {
        var tags = this.tags(stats, queryJob);

        if (stats.getNumChildJobs() != null) {
            runContext.metric(Counter.of("num.child.jobs", stats.getNumChildJobs(), tags));
        }

        runContext.metric(Timer.of("duration", Duration.ofMillis(stats.getEndTime() - stats.getStartTime()), tags));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Project ID",
            description = """
                GCP project ID where the snapshot resides
                """
        )
        private final String projectId;

        @Schema(
            title = "Dataset ID",
            description = """
                Destination dataset ID
                """
        )
        private final String datasetId;

        @Schema(
            title = "Table ID",
            description = """
                Snapshot table name
                """
        )
        private final String tableId;

        @Schema(
            title = "Snapshot time",
            description = """
                Timestamp when the snapshot was recorded
                """
        )
        private final Instant snapshotTime;

        @Schema(
            title = "Expiration time",
            description = """
                Timestamp when the snapshot table will expire
                """
        )
        private final Instant expirationTime;

        @Schema(
            title = "Total rows",
            description = """
                Total number of rows in the table at snapshot time
                """
        )
        private final Long totalRows;

        @Schema(
            title = "Total bytes",
            description = """
                Logical table size in bytes
                """
        )
        private final Long totalBytes;

        @Schema(
            title = "Job ID",
            description = """
                BigQuery job ID executed for the snapshot
                """
        )
        private final String jobId;
    }
}
