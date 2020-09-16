package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.*;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.executions.metrics.Timer;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Extract a BigQuery table to a gcs bucket",
    code = {
        "destinationUris: ",
        "  - \"gs://bucket_name/filename.csv\"",
        "sourceTable: \"my_project.my_dataset.my_table\"",
        "format: CSV",
        "fieldDelimiter: ';'",
        "printHeader: true"
    }
)
@Documentation(
    description = "Extract data from BigQuery table to GCS (Google Cloud Storage)"
)
public class ExtractToGcs extends AbstractBigquery implements RunnableTask<ExtractToGcs.Output>{

    @InputProperty(
        dynamic = true,
        description = "The table to export."
    )
    private String sourceTable;

    @InputProperty(
        dynamic = true,
        description = "The list of fully-qualified Google Cloud Storage URIs (e.g. gs://bucket/path) where " +
            "the extracted table should be written."
    )
    private List<String> destinationUris;

    @InputProperty(
        dynamic = true,
        description = "the compression value to use for exported files. If not set exported files " +
            "are not compressed. "
    )
    private String compression;

    @InputProperty(
        dynamic = true,
        description = "The delimiter to use between fields in the exported data. By default \",\" is used."
    )
    private String fieldDelimiter;

    @InputProperty(
        dynamic = true,
        description = "The exported file format. If not set table is exported in CSV format. "
    )
    private String format;

    @InputProperty(
        description = "[Optional] Flag if format is set to \"AVRO\".",
        body = {"[Optional] If destinationFormat is set to \"AVRO\", this flag indicates whether to enable extracting " +
            "applicable column types (such as TIMESTAMP) to their corresponding AVRO logical " +
            "types (timestamp-micros), instead of only using their raw types (avro-long).",
            "Parameters:",
            "    useAvroLogicalTypes - useAvroLogicalTypes or null for none "}
    )
    private Boolean useAvroLogicalTypes;

    @InputProperty(
        description = "[Optional] Job timeout in milliseconds. If this time limit is exceeded, " +
            "BigQuery may attempt to terminate the job."
    )
    private Long jobTimeoutMs;

    @InputProperty(
        description = "The labels associated with this job.",
        body = {"The labels associated with this job. You can use these to organize and group your jobs. Label " +
            "keys and values can be no longer than 63 characters, can only contain lowercase letters, " +
            "numeric characters, underscores and dashes. International characters are allowed. Label " +
            "values are optional. Label keys must start with a letter and each label in the list must have " +
            "a different key.",
            "Parameters:",
            "    labels - labels or null for none "},
        dynamic = true
    )
    private Map<String,String> labels;

    @InputProperty(
        description = "Whether to print out a header row in the results. By default an header is printed."
    )
    private Boolean printHeader;

    @Override
    public ExtractToGcs.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        ExtractJobConfiguration configuration = this.buildExtractJob(runContext);

        Job extractJob = connection.create(JobInfo.of(configuration));

        logger.debug("Starting query\n{}", JacksonMapper.log(configuration));

        return this.execute(runContext, logger, configuration, extractJob);
    }

    protected ExtractToGcs.Output execute(RunContext runContext, Logger logger, ExtractJobConfiguration configuration, Job job) throws InterruptedException, IllegalVariableEvaluationException, BigQueryException {
        BigQueryService.handleErrors(job, logger);
        job = job.waitFor();
        BigQueryService.handleErrors(job, logger);

        JobStatistics.ExtractStatistics stats = job.getStatistics();
        this.metrics(runContext, stats, job);

        return Output.builder()
            .jobId(job.getJobId().getJob())
            .sourceTable(configuration.getSourceTable().getTable())
            .destinationUris(configuration.getDestinationUris())
            .fileCounts(stats.getDestinationUriFileCounts())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The job id"
        )
        private String jobId;

        @OutputProperty(
            description = "source Table"
        )
        private String sourceTable;

        @OutputProperty(
            description = "The destination URI file"
        )
        private List<String> destinationUris;

        @OutputProperty()
        private List<Long> fileCounts;
    }

    private void metrics(RunContext runContext, JobStatistics.ExtractStatistics stats, Job job) throws IllegalVariableEvaluationException {
        String[] tags = {
            "source_table", runContext.render(this.sourceTable),
            "project_id", job.getJobId().getProject(),
            "location", job.getJobId().getLocation(),
        };

        if (stats.getDestinationUriFileCounts() != null) {
            // Sum of the number of files extracted
            long fileCounts = stats.getDestinationUriFileCounts().stream().mapToLong(Long::longValue).sum();
            runContext.metric(Counter.of("output.file_counts", fileCounts, tags));
        }

        runContext.metric(Timer.of("duration", Duration.ofNanos(stats.getEndTime() - stats.getStartTime()), tags));
    }

    protected ExtractJobConfiguration buildExtractJob(RunContext runContext) throws IllegalVariableEvaluationException {
        ExtractJobConfiguration.Builder builder = ExtractJobConfiguration
            .newBuilder(
                BigQueryService.tableId(runContext.render(this.sourceTable)),
                runContext.render(this.destinationUris)
            );

        if (runContext.render(this.sourceTable) != null){
            builder.setSourceTable(BigQueryService.tableId(runContext.render(this.sourceTable)));
        }

        if (runContext.render(this.destinationUris) != null){
            builder.setDestinationUris(runContext.render(this.destinationUris));
        }

        if (runContext.render(this.compression) != null) {
            builder.setCompression(runContext.render(runContext.render(this.compression)));
        }

        if (runContext.render(this.fieldDelimiter) != null) {
            builder.setFieldDelimiter(runContext.render(this.fieldDelimiter));
        }

        if (runContext.render(this.format) != null) {
            builder.setFormat(runContext.render(this.format));
        }

        if (this.printHeader != null) {
            builder.setPrintHeader(this.printHeader);
        }

        if (this.jobTimeoutMs != null) {
            builder.setJobTimeoutMs(this.jobTimeoutMs);
        }

        if (this.useAvroLogicalTypes != null) {
            builder.setUseAvroLogicalTypes(this.useAvroLogicalTypes);
        }

        if (this.labels != null) {
            builder.setLabels(this.labels);
        }

        return builder.build();
    }
}

