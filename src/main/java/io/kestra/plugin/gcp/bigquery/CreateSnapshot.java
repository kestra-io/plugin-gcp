package io.kestra.plugin.gcp.bigquery;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a BigQuery table snapshot",
    description = "Creates a point-in-time, read-only table snapshot in Google Cloud BigQuery with optional expiration (TTL) using a copy job."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: bigquery_snapshot
                namespace: company.data

                tasks:
                  - id: create_snapshot
                    type: io.kestra.plugin.gcp.bigquery.CreateSnapshot
                    projectId: my-project
                    sourceDataset: prod_dataset
                    sourceTable: orders
                    destinationDataset: backup_dataset
                    snapshotTable: orders_snapshot
                    expirationDuration: P7D
                """
        )
    },
    metrics = {
        @Metric(name = "num.child.jobs", type = Counter.TYPE, description = "The number of child jobs executed."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time it took for the snapshot copy job to run.")
    }
)
public class CreateSnapshot extends AbstractJob implements RunnableTask<CreateSnapshot.Output> {
    @NotNull
    @Schema(
        title = "Source dataset ID",
        description = "The dataset containing the base table to snapshot."
    )
    @PluginProperty(group = "main")
    private Property<String> sourceDataset;

    @NotNull
    @Schema(
        title = "Source table name",
        description = "The name of the base table to snapshot."
    )
    @PluginProperty(group = "main")
    private Property<String> sourceTable;

    @Schema(
        title = "Destination dataset ID",
        description = "The dataset where the snapshot table will be created. If not specified, defaults to the source dataset."
    )
    @PluginProperty(group = "main")
    private Property<String> destinationDataset;

    @NotNull
    @Schema(
        title = "Snapshot table name",
        description = "The name of the snapshot table to create."
    )
    @PluginProperty(group = "main")
    private Property<String> snapshotTable;

    @Schema(
        title = "Expiration duration",
        description = "Duration from creation until the snapshot table is automatically deleted by BigQuery (e.g. `P7D`, `P30D`). If unset, the snapshot persists indefinitely."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> expirationDuration;

    @Schema(
        title = "Historical snapshot timestamp",
        description = "Optional point-in-time timestamp within BigQuery's time-travel window (up to 7 days) to snapshot historical table data."
    )
    @PluginProperty(group = "advanced")
    private Property<Instant> snapshotTimestamp;

    @Schema(
        title = "Table description",
        description = "A user-friendly description of the snapshot table."
    )
    @PluginProperty(dynamic = true, group = "advanced")
    protected String description;

    @Schema(
        title = "Friendly name",
        description = "A descriptive display name for the snapshot table."
    )
    @PluginProperty(group = "advanced")
    private Property<String> friendlyName;

    @Override
    public CreateSnapshot.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        CopyJobConfiguration jobConfiguration = this.jobConfiguration(runContext);
        TableId destinationTableId = jobConfiguration.getDestinationTable();

        logger.debug("Starting snapshot copy from {} to {}", jobConfiguration.getSourceTables(), destinationTableId);

        boolean isDryRun = runContext.render(this.dryRun).as(Boolean.class).orElse(false);

        Job copyJob = this.waitForJob(
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

        JobStatistics.CopyStatistics copyJobStatistics = copyJob.getStatistics();
        if (copyJobStatistics != null) {
            this.metrics(runContext, copyJobStatistics, copyJob);
        }

        Output.OutputBuilder output = Output.builder()
            .jobId(copyJob.getJobId().getJob())
            .projectId(destinationTableId.getProject())
            .datasetId(destinationTableId.getDataset())
            .tableId(destinationTableId.getTable());

        if (!isDryRun) {
            Table createdTable = connection.getTable(destinationTableId);
            if (createdTable != null) {
                if (this.description != null || this.friendlyName != null) {
                    Table.Builder updateBuilder = createdTable.toBuilder();
                    if (this.description != null) {
                        updateBuilder.setDescription(runContext.render(this.description));
                    }
                    if (this.friendlyName != null) {
                        updateBuilder.setFriendlyName(runContext.render(this.friendlyName).as(String.class).orElseThrow());
                    }
                    createdTable = updateBuilder.build().update();
                }

                output.projectId(createdTable.getTableId().getProject());

                Instant creationTime = createdTable.getCreationTime() == null ? null : Instant.ofEpochMilli(createdTable.getCreationTime());
                output.creationTime(creationTime);
                output.expirationTime(createdTable.getExpirationTime() == null ? null : Instant.ofEpochMilli(createdTable.getExpirationTime()));
                output.totalRows(createdTable.getNumRows());
                output.totalBytes(createdTable.getNumTotalLogicalBytes() != null ? createdTable.getNumTotalLogicalBytes() : createdTable.getNumBytes());
                output.friendlyName(createdTable.getFriendlyName());
                output.description(createdTable.getDescription());

                if (createdTable.getDefinition() instanceof SnapshotTableDefinition snapshotDef && snapshotDef.getSnapshotTime() != null) {
                    output.snapshotTime(Instant.parse(snapshotDef.getSnapshotTime()));
                } else if (this.snapshotTimestamp != null) {
                    output.snapshotTime(runContext.render(this.snapshotTimestamp).as(Instant.class).orElse(null));
                } else {
                    output.snapshotTime(creationTime);
                }
            }
        }

        return output.build();
    }

    protected CopyJobConfiguration jobConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        TableId destinationTableId = this.destinationTableId(runContext);
        TableId sourceTableId = this.sourceTableId(runContext);

        CopyJobConfiguration.Builder builder = CopyJobConfiguration.newBuilder(
            destinationTableId,
            List.of(sourceTableId)
        );

        builder.setOperationType("SNAPSHOT");

        if (this.expirationDuration != null) {
            Duration duration = runContext.render(this.expirationDuration).as(Duration.class).orElseThrow();
            Instant expirationInstant = Instant.now().plus(duration);
            builder.setDestinationExpirationTime(DateTimeFormatter.ISO_INSTANT.format(expirationInstant));
        }

        if (this.writeDisposition != null) {
            builder.setWriteDisposition(runContext.render(this.writeDisposition).as(JobInfo.WriteDisposition.class).orElseThrow());
        }

        if (this.createDisposition != null) {
            builder.setCreateDisposition(runContext.render(this.createDisposition).as(JobInfo.CreateDisposition.class).orElseThrow());
        }

        if (this.jobTimeout != null) {
            builder.setJobTimeoutMs(runContext.render(this.jobTimeout).as(Duration.class).orElseThrow().toMillis());
        }

        Map<String, String> finalLabels = new HashMap<>(BigQueryService.labels(runContext));
        if (this.labels != null) {
            var renderedLabels = runContext.render(this.labels).asMap(String.class, String.class);
            if (!renderedLabels.isEmpty()) {
                finalLabels.putAll(renderedLabels);
            }
        }
        builder.setLabels(finalLabels);

        return builder.build();
    }

    protected TableId sourceTableId(RunContext runContext) throws IllegalVariableEvaluationException {
        String project = this.projectId != null ? runContext.render(this.projectId).as(String.class).orElse(null) : null;
        String sourceDs = runContext.render(this.sourceDataset).as(String.class).orElseThrow();
        String sourceTbl = runContext.render(this.sourceTable).as(String.class).orElseThrow();

        if (this.snapshotTimestamp != null) {
            Instant instant = runContext.render(this.snapshotTimestamp).as(Instant.class).orElseThrow();
            sourceTbl = sourceTbl + "@" + instant.toEpochMilli();
        }

        return project != null ? TableId.of(project, sourceDs, sourceTbl) : TableId.of(sourceDs, sourceTbl);
    }

    protected TableId destinationTableId(RunContext runContext) throws IllegalVariableEvaluationException {
        String project = this.projectId != null ? runContext.render(this.projectId).as(String.class).orElse(null) : null;
        String sourceDs = runContext.render(this.sourceDataset).as(String.class).orElseThrow();
        String destDs = this.destinationDataset != null
            ? runContext.render(this.destinationDataset).as(String.class).orElse(sourceDs)
            : sourceDs;
        String snapTbl = runContext.render(this.snapshotTable).as(String.class).orElseThrow();

        return project != null ? TableId.of(project, destDs, snapTbl) : TableId.of(destDs, snapTbl);
    }

    private String[] tags(JobStatistics.CopyStatistics stats, Job queryJob) {
        return new String[] {
            "project_id", queryJob.getJobId().getProject(),
            "location", queryJob.getJobId().getLocation(),
        };
    }

    private void metrics(RunContext runContext, JobStatistics.CopyStatistics stats, Job queryJob) {
        String[] tags = this.tags(stats, queryJob);

        if (stats.getNumChildJobs() != null) {
            runContext.metric(Counter.of("num.child.jobs", stats.getNumChildJobs(), tags));
        }

        if (stats.getEndTime() != null && stats.getStartTime() != null) {
            runContext.metric(Timer.of("duration", Duration.ofMillis(stats.getEndTime() - stats.getStartTime()), tags));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Job ID"
        )
        private final String jobId;

        @Schema(
            title = "Project ID",
            description = "GCP Project ID where the snapshot resides."
        )
        private final String projectId;

        @Schema(
            title = "Dataset ID",
            description = "Destination dataset ID."
        )
        private final String datasetId;

        @Schema(
            title = "Table ID",
            description = "Name of the created snapshot table."
        )
        private final String tableId;

        @Schema(
            title = "Creation time",
            description = "Exact timestamp when the snapshot table was created in BigQuery."
        )
        private final Instant creationTime;

        @Schema(
            title = "Snapshot point-in-time timestamp",
            description = "Point-in-time timestamp from which the snapshot was captured."
        )
        private final Instant snapshotTime;

        @Schema(
            title = "Expiration time",
            description = "Timestamp when the snapshot will automatically expire and be deleted by BigQuery. Null if the table persists indefinitely."
        )
        private final Instant expirationTime;

        @Schema(
            title = "Total rows",
            description = "Total number of rows in the snapshot table."
        )
        private final BigInteger totalRows;

        @Schema(
            title = "Total logical bytes",
            description = "Total logical size of the table in bytes."
        )
        private final Long totalBytes;

        @Schema(
            title = "Friendly name"
        )
        private final String friendlyName;

        @Schema(
            title = "Description"
        )
        private final String description;
    }
}
