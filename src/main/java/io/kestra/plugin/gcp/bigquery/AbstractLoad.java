package io.kestra.plugin.gcp.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.*;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@LoadCsvValidation
abstract public class AbstractLoad extends AbstractBigquery implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "The table where to put query results.",
        description = "If not provided, a new table is created."
    )
    protected Property<String> destinationTable;

    @Schema(
        title = "The clustering specification for the destination table."
    )
    private Property<List<String>> clusteringFields;

    @Schema(
        title = "[Experimental] Options allowing the schema of the destination table to be updated as a side effect of the query job.",
        description = "Schema update options are supported in two cases: when" +
            " writeDisposition is WRITE_APPEND; when writeDisposition is WRITE_TRUNCATE and the destination" +
            " table is a partition of a table, specified by partition decorators. For normal tables," +
            " WRITE_TRUNCATE will always overwrite the schema."
    )
    private Property<List<JobInfo.SchemaUpdateOption>> schemaUpdateOptions;

    @Schema(
        title = "The time partitioning field for the destination table."
    )
    private Property<String> timePartitioningField;

    @Schema(
        title = "The time partitioning type specification for the destination table."
    )
    @Builder.Default
    private Property<TimePartitioning.Type> timePartitioningType = Property.ofValue(TimePartitioning.Type.DAY);


    @Schema(
        title = "The action that should occur if the destination table already exists."
    )
    private Property<JobInfo.WriteDisposition> writeDisposition;

    @Schema(
        title = "[Experimental] Automatic inference of the options and schema for CSV and JSON sources."
    )
    private Property<Boolean> autodetect;

    @Schema(
        title = "Whether the job is allowed to create tables."
    )
    private Property<JobInfo.CreateDisposition> createDisposition;

    @Schema(
        title = "Whether BigQuery should allow extra values that are not represented in the table schema.",
        description = " If true, the extra values are ignored. If false, records with extra columns" +
            " are treated as bad records, and if there are too many bad records, an invalid error is" +
            " returned in the job result. By default unknown values are not allowed."
    )
    private Property<Boolean> ignoreUnknownValues;

    @Schema(
        title = "The maximum number of bad records that BigQuery can ignore when running the job.",
        description = " If the number of bad records exceeds this value, an invalid error is returned in the job result." +
            " By default, no bad record is ignored."
    )
    private Property<Integer> maxBadRecords;

    @Schema(
        title = "The schema for the destination table.",
        description = "The schema can be omitted if the destination table" +
            " already exists, or if you're loading data from a Google Cloud Datastore backup (i.e. " +
            " DATASTORE_BACKUP format option).\n" +
            "```yaml\n" +
            "schema:\n" +
            "  fields:\n" +
            "    - name: colA\n" +
            "      type: STRING\n" +
            "    - name: colB\n" +
            "      type: NUMERIC\n"+
            "```\n" +
            "See type from [StandardSQLTypeName](https://javadoc.io/static/com.google.cloud/google-cloud-bigquery/1.88.0/com/google/cloud/bigquery/StandardSQLTypeName.html)"
    )
    private Property<Map<String, Object>> schema;

    @Schema(
        title = "The source format, and possibly some parsing options, of the external data."
    )
    private Format format;

    @Schema(
        title = "Csv parsing options."
    )
    private CsvOptions csvOptions;

    @Schema(
        title = "Avro parsing options."
    )
    private AvroOptions avroOptions;

    @SuppressWarnings("DuplicatedCode")
    protected void setOptions(LoadConfiguration.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException, JsonProcessingException {
        if (this.clusteringFields != null) {
            builder.setClustering(Clustering.newBuilder().setFields(runContext.render(this.clusteringFields).asList(String.class)).build());
        }

        if (this.schemaUpdateOptions != null) {
            builder.setSchemaUpdateOptions(runContext.render(this.schemaUpdateOptions).asList(JobInfo.SchemaUpdateOption.class));
        }

        if (this.timePartitioningField != null) {
            builder.setTimePartitioning(TimePartitioning.newBuilder(runContext.render(this.timePartitioningType).as(TimePartitioning.Type.class).orElseThrow())
                .setField(runContext.render(runContext.render(this.timePartitioningField).as(String.class).orElseThrow()))
                .build()
            );
        }

        if (this.writeDisposition != null) {
            builder.setWriteDisposition(runContext.render(this.writeDisposition).as(JobInfo.WriteDisposition.class).orElseThrow());
        }

        if (this.autodetect != null) {
            builder.setAutodetect(runContext.render(autodetect).as(Boolean.class).orElseThrow());
        }

        if (this.createDisposition != null) {
            builder.setCreateDisposition(runContext.render(this.createDisposition).as(JobInfo.CreateDisposition.class).orElseThrow());
        }

        if (this.ignoreUnknownValues != null) {
            builder.setIgnoreUnknownValues(runContext.render(this.ignoreUnknownValues).as(Boolean.class).orElseThrow());
        }

        if (this.maxBadRecords != null) {
            builder.setMaxBadRecords(runContext.render(this.maxBadRecords).as(Integer.class).orElseThrow());
        }

        if (this.schema != null) {
            builder.setSchema(schema(runContext.render(this.schema).asMap(String.class, Object.class)));
        }

        switch (this.format) {
            case CSV:
                builder.setFormatOptions(this.csvOptions.to(runContext));
                break;
            case JSON:
                builder.setFormatOptions(FormatOptions.json());
                break;
            case AVRO:
                builder.setFormatOptions(FormatOptions.avro());

                if (this.avroOptions != null && this.avroOptions.useAvroLogicalTypes != null) {
                    builder.setUseAvroLogicalTypes(runContext.render(this.avroOptions.useAvroLogicalTypes).as(Boolean.class).orElseThrow());
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

    @SuppressWarnings("unchecked")
    private com.google.cloud.bigquery.Schema schema(Map<String, Object> schema)  {

        if (!schema.containsKey("fields")) {
            throw new IllegalArgumentException("Unable to deserialize schema, no 'fields' with data '" + schema + "'");
        }

        return com.google.cloud.bigquery.Schema.of(fields((List<Map<String, Object>>) schema.get("fields")));
    }

    @SuppressWarnings("unchecked")
    private List<Field> fields(List<Map<String, Object>> fields)  {
        return fields
            .stream()
            .map(r -> Field
                .newBuilder(
                    (String) r.get("name"),
                    StandardSQLTypeName.valueOf((String) r.get("type")),
                    r.containsKey("fields") ? FieldList.of(fields((List<Map<String, Object>>) r.get("fields"))) : null
                )
                .setDescription(r.containsKey("description") ? (String) r.get("description") : null)
                .setMode(r.containsKey("mode") ? Field.Mode.valueOf((String) r.get("mode")) : null)
                .build()
            )
            .collect(Collectors.toList());
    }

    protected Output outputs(RunContext runContext, LoadConfiguration configuration, Job job) throws InterruptedException, IllegalVariableEvaluationException, BigQueryException {
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The job id"
        )
        private final String jobId;

        @Schema(
            title = "Destination table"
        )
        private final String destinationTable;

        @Schema(
            title = "Output rows count"
        )
        private final Long rows;
    }

    private void metrics(RunContext runContext, JobStatistics.LoadStatistics stats, Job job) throws IllegalVariableEvaluationException {
        String[] tags = {
            "destination_table", runContext.render(this.destinationTable).as(String.class).orElse(null),
            "project_id", job.getJobId().getProject(),
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

        runContext.metric(Timer.of("duration", Duration.ofMillis(stats.getEndTime() - stats.getStartTime()), tags));
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
        @Schema(
            title = "Whether BigQuery should accept rows that are missing trailing optional columns.",
            description = "If true, BigQuery treats missing trailing columns as null values. If {@code false}, records" +
                " with missing trailing columns are treated as bad records, and if there are too many bad" +
                " records, an invalid error is returned in the job result. By default, rows with missing" +
                " trailing columns are considered bad records."
        )
        private Property<Boolean> allowJaggedRows;

        @Schema(
            title = "Whether BigQuery should allow quoted data sections that contain newline characters in a CSV file.",
            description = "By default quoted newline are not allowed."
        )
        private Property<Boolean> allowQuotedNewLines;

        @Schema(
            title = "The character encoding of the data.",
            description = "The supported values are UTF-8 or ISO-8859-1. The" +
                " default value is UTF-8. BigQuery decodes the data after the raw, binary data has been split" +
                " using the values set in {@link #setQuote(String)} and {@link #setFieldDelimiter(String)}."
        )
        private Property<String> encoding;

        @Schema(
            title = "The separator for fields in a CSV file.",
            description = "BigQuery converts the string to ISO-8859-1" +
                " encoding, and then uses the first byte of the encoded string to split the data in its raw," +
                " binary state. BigQuery also supports the escape sequence \"\\t\" to specify a tab separator. The" +
                " default value is a comma (',')."
        )
        private Property<String> fieldDelimiter;

        @Schema(
            title = "The value that is used to quote data sections in a CSV file.",
            description = "BigQuery converts the" +
                " string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split" +
                " the data in its raw, binary state. The default value is a double-quote ('\"'). If your data" +
                " does not contain quoted sections, set the property value to an empty string. If your data" +
                " contains quoted newline characters, you must also set {@link" +
                " #setAllowQuotedNewLines(boolean)} property to {@code true}."
        )
        private Property<String> quote;

        @Schema(
            title = "The number of rows at the top of a CSV file that BigQuery will skip when reading the data",
            description = "The default value is 0. This property is useful if you have header rows in the file" +
                " that should be skipped."
        )
        private Property<Long> skipLeadingRows;

        private com.google.cloud.bigquery.CsvOptions to(RunContext runContext) throws IllegalVariableEvaluationException {
            com.google.cloud.bigquery.CsvOptions.Builder builder = com.google.cloud.bigquery.CsvOptions.newBuilder();

            if (this.allowJaggedRows != null) {
                builder.setAllowJaggedRows(runContext.render(this.allowJaggedRows).as(Boolean.class).orElseThrow());
            }

            if (this.allowQuotedNewLines != null) {
                builder.setAllowQuotedNewLines(runContext.render(this.allowQuotedNewLines).as(Boolean.class).orElseThrow());
            }

            if (this.encoding != null) {
                builder.setEncoding(runContext.render(this.encoding).as(String.class).orElseThrow());
            }

            if (this.fieldDelimiter != null) {
                builder.setFieldDelimiter(runContext.render(this.fieldDelimiter).as(String.class).orElseThrow());
            }

            if (this.quote != null) {
                builder.setQuote(runContext.render(this.quote).as(String.class).orElseThrow());
            }

            if (this.skipLeadingRows != null) {
                builder.setSkipLeadingRows(runContext.render(this.skipLeadingRows).as(Long.class).orElseThrow());
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
        @Schema(
            title = "If format is set to AVRO, you can interpret logical types into their corresponding" +
                " types (such as TIMESTAMP) instead of only using their raw types (such as INTEGER)",
            description = "The value may be null."
        )
        private Property<Boolean> useAvroLogicalTypes;
    }
}
