package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.*;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
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
    }
)
@Schema(
    title = "Extract data from BigQuery table to GCS (Google Cloud Storage)"
)
public class ExtractToGcs extends AbstractBigquery implements RunnableTask<ExtractToGcs.Output>{

    @Schema(
        title = "The table to export."
    )
    @PluginProperty(dynamic = true)
    private String sourceTable;

    @Schema(
        title = "The list of fully-qualified Google Cloud Storage URIs (e.g. gs://bucket/path) where " +
            "the extracted table should be written."
    )
    @PluginProperty(dynamic = true)
    private List<String> destinationUris;

    @Schema(
        title = "the compression value to use for exported files. If not set exported files " +
            "are not compressed. "
    )
    @PluginProperty(dynamic = true)
    private String compression;

    @Schema(
        title = "The delimiter to use between fields in the exported data. By default \",\" is used."
    )
    @PluginProperty(dynamic = true)
    private String fieldDelimiter;

    @Schema(
        title = "The exported file format. If not set table is exported in CSV format. "
    )
    @PluginProperty(dynamic = true)
    private String format;

    @Schema(
        title = "[Optional] Flag if format is set to \"AVRO\".",
        description = "[Optional] If destinationFormat is set to \"AVRO\", this flag indicates whether to enable extracting " +
            "applicable column types (such as TIMESTAMP) to their corresponding AVRO logical " +
            "types (timestamp-micros), instead of only using their raw types (avro-long). \n" +
            "Parameters:\n"+
            "    useAvroLogicalTypes - useAvroLogicalTypes or null for none "
    )
    private Boolean useAvroLogicalTypes;

    @Schema(
        title = "[Optional] Job timeout in milliseconds. If this time limit is exceeded, " +
            "BigQuery may attempt to terminate the job."
    )
    private Long jobTimeoutMs;

    @Schema(
        title = "The labels associated with this job.",
        description = "The labels associated with this job. You can use these to organize and group your jobs. Label " +
            "keys and values can be no longer than 63 characters, can only contain lowercase letters, " +
            "numeric characters, underscores and dashes. International characters are allowed. Label " +
            "values are optional. Label keys must start with a letter and each label in the list must have " +
            "a different key.\n" +
            "Parameters:\n" +
            "    labels - labels or null for none "
    )
    @PluginProperty(dynamic = true)
    private Map<String,String> labels;

    @Schema(
        title = "Whether to print out a header row in the results. By default an header is printed."
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The job id"
        )
        private final String jobId;

        @Schema(
            title = "source Table"
        )
        private final String sourceTable;

        @Schema(
            title = "The destination URI file"
        )
        private final List<String> destinationUris;

        @Schema(title = "Number of extracted files")
        private final List<Long> fileCounts;
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

