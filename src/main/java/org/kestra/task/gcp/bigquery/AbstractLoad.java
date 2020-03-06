package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.*;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.executions.metrics.Timer;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractLoad extends AbstractBigquery implements RunnableTask<AbstractLoad.Output> {
    @InputProperty(
        description = "The table where to put query results",
        body = "If not provided a new table is created.",
        dynamic = true
    )
    protected String destinationTable;

    @InputProperty(
        description = "The clustering specification for the destination table"
    )
    private List<String> clusteringFields;

    @InputProperty(
        description = "[Experimental] Options allowing the schema of the destination table to be updated as a side effect of the query job",
        body = "Schema update options are supported in two cases: when\n" +
            " writeDisposition is WRITE_APPEND; when writeDisposition is WRITE_TRUNCATE and the destination\n" +
            " table is a partition of a table, specified by partition decorators. For normal tables,\n" +
            " WRITE_TRUNCATE will always overwrite the schema."
    )
    private List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    @InputProperty(
        description = "The time partitioning specification for the destination table"
    )
    private String timePartitioningField;

    @InputProperty(
        description = "The action that should occur if the destination table already exists"
    )
    private JobInfo.WriteDisposition writeDisposition;

    @InputProperty(
        description = "[Experimental] Automatic inference of the options and schema for CSV and JSON sources"
    )
    private Boolean autodetect;

    @InputProperty(
        description = "Whether the job is allowed to create tables"
    )
    private JobInfo.CreateDisposition createDisposition;

    @InputProperty(
        description = "Whether BigQuery should allow extra values that are not represented in the table schema",
        body = " If true, the extra values are ignored. If false, records with extra columns\n" +
            " are treated as bad records, and if there are too many bad records, an invalid error is\n" +
            " returned in the job result. By default unknown values are not allowed."
    )
    private Boolean ignoreUnknownValues;

    @InputProperty(
        description = "The maximum number of bad records that BigQuery can ignore when running the job",
        body = " If the number of bad records exceeds this value, an invalid error is returned in the job result.\n" +
            " By default no bad record is ignored."
    )
    private Integer maxBadRecords;

    @InputProperty(
        description = "The schema for the destination table",
        body = "The schema can be omitted if the destination table\n" +
            " already exists, or if you're loading data from a Google Cloud Datastore backup (i.e. \n" +
            " DATASTORE_BACKUP format option)."
    )
    private Schema schema;

    @InputProperty(
        description = "The source format, and possibly some parsing options, of the external data"
    )
    private Format format;

    @InputProperty(
        description = "Csv parsing options"
    )
    private CsvOptions csvOptions;

    @InputProperty(
        description = "Avro parsing options"
    )
    private AvroOptions avroOptions;

    @SuppressWarnings("DuplicatedCode")
    protected void setOptions(LoadConfiguration.Builder builder) {
        if (this.clusteringFields != null) {
            builder.setClustering(Clustering.newBuilder().setFields(this.clusteringFields).build());
        }

        if (this.schemaUpdateOptions != null) {
            builder.setSchemaUpdateOptions(this.schemaUpdateOptions);
        }

        if (this.timePartitioningField != null) {
            builder.setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField(this.timePartitioningField)
                .build()
            );
        }

        if (this.writeDisposition != null) {
            builder.setWriteDisposition(this.writeDisposition);
        }

        if (this.autodetect != null) {
            builder.setAutodetect(autodetect);
        }

        if (this.createDisposition != null) {
            builder.setCreateDisposition(this.createDisposition);
        }

        if (this.ignoreUnknownValues != null) {
            builder.setIgnoreUnknownValues(this.ignoreUnknownValues);
        }

        if (this.maxBadRecords != null) {
            builder.setMaxBadRecords(this.maxBadRecords);
        }

        if (this.schema != null) {
            builder.setSchema(this.schema);
        }

        switch (this.format) {
            case CSV:
                builder.setFormatOptions(this.csvOptions.to());
                break;
            case JSON:
                builder.setFormatOptions(FormatOptions.json());
                break;
            case AVRO:
                builder.setFormatOptions(FormatOptions.avro());

                if (this.avroOptions != null && this.avroOptions.useAvroLogicalTypes != null) {
                    builder.setUseAvroLogicalTypes(this.avroOptions.useAvroLogicalTypes);
                }
                break;
            case PARQUET:
                builder.setFormatOptions(FormatOptions.parquet());
                break;
            case ORC:
                builder.setFormatOptions(FormatOptions.orc());
                break;
        }
    }

    protected Output execute(RunContext runContext, Logger logger, LoadConfiguration configuration, Job job) throws InterruptedException, IOException, IllegalVariableEvaluationException{
        Connection.handleErrors(job, logger);
        job = job.waitFor();
        Connection.handleErrors(job, logger);

        JobStatistics.LoadStatistics stats = job.getStatistics();
        this.metrics(runContext, stats, job);

        return Output.builder()
            .jobId(job.getJobId().getJob())
            .rows(stats.getOutputRows())
            .destinationTable(configuration.getDestinationTable().getProject() + "." +
                configuration.getDestinationTable().getDataset() + "." +
                configuration.getDestinationTable().getTable())
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
            description = "Destination table"
        )
        private String destinationTable;

        @OutputProperty(
            description = "Output rows count"
        )
        private Long rows;
    }

    private void metrics(RunContext runContext, JobStatistics.LoadStatistics stats, Job job) throws IllegalVariableEvaluationException {
        String[] tags = {
            "destination_table", runContext.render(this.destinationTable),
            "projectId", job.getJobId().getProject(),
            "location", job.getJobId().getLocation(),
        };

        if (stats.getOutputRows() != null) {
            runContext.metric(Counter.of("output.rows", stats.getOutputRows(), tags));
        }

        if (stats.getOutputBytes() != null) {
            runContext.metric(Counter.of("output.bytes", stats.getOutputBytes(), tags));
        }

        if (stats.getBadRecords() != null) {
            runContext.metric(Counter.of("bad.records", stats.getBadRecords(), tags));
        }

        if (stats.getInputBytes() != null) {
            runContext.metric(Counter.of("input.bytes", stats.getInputBytes(), tags));
        }

        if (stats.getInputFiles() != null) {
            runContext.metric(Counter.of("input.files", stats.getInputFiles(), tags));
        }


        runContext.metric(Timer.of("duration", Duration.ofNanos(stats.getEndTime() - stats.getStartTime()), tags));
    }

    public enum Format {
        CSV,
        JSON,
        AVRO,
        PARQUET,
        ORC,
        // GOOGLE_SHEETS,
        // BIGTABLE,
        // DATASTORE_BACKUP,
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CsvOptions {
        @InputProperty(
            description = "Whether BigQuery should accept rows that are missing trailing optional columns",
            body = "If true, BigQuery treats missing trailing columns as null values. If {@code false}, records\n" +
                " with missing trailing columns are treated as bad records, and if there are too many bad\n" +
                " records, an invalid error is returned in the job result. By default, rows with missing\n" +
                " trailing columns are considered bad records.",
            dynamic = true
        )
        private Boolean allowJaggedRows;

        @InputProperty(
            description = "Whether BigQuery should allow quoted data sections that contain newline characters in a CSV file",
            body = "By default quoted newline are not allowed.",
            dynamic = true
        )
        private Boolean allowQuotedNewLines;

        @InputProperty(
            description = "The character encoding of the data",
            body = "The supported values are UTF-8 or ISO-8859-1. The\n" +
                " default value is UTF-8. BigQuery decodes the data after the raw, binary data has been split\n" +
                " using the values set in {@link #setQuote(String)} and {@link #setFieldDelimiter(String)}.",
            dynamic = true
        )
        private String encoding;

        @InputProperty(
            description = "The separator for fields in a CSV file",
            body = "BigQuery converts the string to ISO-8859-1\n" +
                " encoding, and then uses the first byte of the encoded string to split the data in its raw,\n" +
                " binary state. BigQuery also supports the escape sequence \"\\t\" to specify a tab separator. The\n" +
                " default value is a comma (',').",
            dynamic = true
        )
        private String fieldDelimiter;

        @InputProperty(
            description = "The value that is used to quote data sections in a CSV file",
            body = "BigQuery converts the\n" +
                " string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split\n" +
                " the data in its raw, binary state. The default value is a double-quote ('\"'). If your data\n" +
                " does not contain quoted sections, set the property value to an empty string. If your data\n" +
                " contains quoted newline characters, you must also set {@link\n" +
                " #setAllowQuotedNewLines(boolean)} property to {@code true}.",
            dynamic = true
        )
        private String quote;

        @InputProperty(
            description = "The number of rows at the top of a CSV file that BigQuery will skip when reading the data",
            body = "The default value is 0. This property is useful if you have header rows in the file\n" +
                " that should be skipped.",
            dynamic = true
        )
        private Long skipLeadingRows;

        private com.google.cloud.bigquery.CsvOptions to() {
            com.google.cloud.bigquery.CsvOptions.Builder builder = com.google.cloud.bigquery.CsvOptions.newBuilder();

            if (this.allowJaggedRows != null) {
                builder.setAllowJaggedRows(this.allowJaggedRows);
            }

            if (this.allowQuotedNewLines != null) {
                builder.setAllowQuotedNewLines(this.allowQuotedNewLines);
            }

            if (this.encoding != null) {
                builder.setEncoding(this.encoding);
            }

            if (this.fieldDelimiter != null) {
                builder.setFieldDelimiter(this.fieldDelimiter);
            }

            if (this.quote != null) {
                builder.setQuote(this.quote);
            }

            if (this.skipLeadingRows != null) {
                builder.setSkipLeadingRows(this.skipLeadingRows);
            }

            return builder.build();
        }
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AvroOptions {

        @InputProperty(
            description = "If Format option is set to AVRO, you can interpret logical types into their corresponding\n" +
                " types (such as TIMESTAMP) instead of only using their raw types (such as INTEGER)",
            body = "The value may be null.",
            dynamic = true
        )
        private Boolean useAvroLogicalTypes;
    }
}
