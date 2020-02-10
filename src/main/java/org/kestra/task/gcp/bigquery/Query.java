package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.*;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.executions.metrics.Timer;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Create a table with a custom query",
    code = {
        "destinationTable: \"my_project.my_dataset.my_table\"",
        "writeDisposition: WRITE_APPEND",
        "sql: |",
        "  SELECT ",
        "    \"hello\" as string,",
        "    NULL AS `nullable`,",
        "    1 as int,",
        "    1.25 AS float,",
        "    DATE(\"2008-12-25\") AS date,",
        "    DATETIME \"2008-12-25 15:30:00.123456\" AS datetime,",
        "    TIME(DATETIME \"2008-12-25 15:30:00.123456\") AS time,",
        "    TIMESTAMP(\"2008-12-25 15:30:00.123456\") AS timestamp,",
        "    ST_GEOGPOINT(50.6833, 2.9) AS geopoint,",
        "    ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,",
        "    STRUCT(4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`"
    }
)
@Example(
    full = true,
    title = "Execute a query and fetch results sets on another task",
    code = {
        "tasks:",
        "- id: fetch",
        "  type: org.kestra.task.gcp.bigquery.Query",
        "  fetch: true",
        "  sql: |",
        "    SELECT 1 as id, \"John\" as name",
        "    UNION ALL",
        "    SELECT 2 as id, \"Doe\" as name",
        "- id: use-fetched-data",
        "  type: org.kestra.core.tasks.debugs.Return",
        "  format: |",
        "    {{#each outputs.fetch.rows}}",
        "    id : {{ this.id }}, name: {{ this.name }}",
        "    {{/each}}"
    }
)
public class Query extends Task implements RunnableTask<Query.Output> {
    private String sql;

    private String projectId;

    @Builder.Default
    private boolean legacySql = false;

    @Builder.Default
    private boolean fetch = false;

    @Builder.Default
    private boolean fetchOne = false;

    private List<String> positionalParameters;

    private Map<String, String> namedParameters;

    private List<String> clusteringFields;

    private String destinationTable;

    private List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    private String timePartitioningField;

    private JobInfo.WriteDisposition writeDisposition;

    private JobInfo.CreateDisposition createDisposition;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        BigQuery connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());

        QueryJobConfiguration jobConfiguration = this.jobConfiguration(runContext);

        logger.debug("Starting query\n{}", JacksonMapper.log(jobConfiguration));

        Job queryJob = connection
            .create(JobInfo.newBuilder(jobConfiguration)
                .setJobId(Connection.jobId(runContext))
                .build()
            );

        Connection.handleErrors(queryJob, logger);
        queryJob = queryJob.waitFor();
        Connection.handleErrors(queryJob, logger);

        this.metrics(runContext, queryJob.getStatistics(), queryJob);

        Output.OutputBuilder output = Output.builder()
            .jobId(queryJob.getJobId().getJob());

        if (this.fetch || this.fetchOne) {
            TableResult result = queryJob.getQueryResults();

            if (fetch) {
                output.rows(this.fetchResult(result));
            } else {
                output.row(this.fetchResult(result).get(0));
            }
        }

        return output.build();
    }

    protected QueryJobConfiguration jobConfiguration(RunContext runContext) throws IOException {
        String sql = runContext.render(this.sql);

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(sql)
            .setUseLegacySql(this.legacySql);

        if (this.clusteringFields != null) {
            builder.setClustering(Clustering.newBuilder().setFields(this.clusteringFields).build());
        }

        if (this.destinationTable != null) {
            builder.setDestinationTable(Connection.tableId(runContext.render(this.destinationTable)));
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

        if (this.createDisposition != null) {
            builder.setCreateDisposition(this.createDisposition);
        }

        return builder.build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        private String jobId;
        private List<Map<String, Object>> rows;
        private Map<String, Object> row;
    }

    private void metrics(RunContext runContext, JobStatistics.QueryStatistics stats, Job queryJob) throws IOException {
        String[] tags = {
            "statement_type", stats.getStatementType().name(),
            "fetch", this.fetch || this.fetchOne ? "true" : "false",
            "projectId", queryJob.getJobId().getProject(),
            "location", queryJob.getJobId().getLocation(),
        };

        if (this.destinationTable != null) {
            ArrayUtils.addAll(tags, "destination_table", runContext.render(this.destinationTable));
        }

        if (stats.getEstimatedBytesProcessed() != null) {
            runContext.metric(Counter.of("estimated.bytes.processed", stats.getEstimatedBytesProcessed(), tags));
        }

        if (stats.getNumDmlAffectedRows() != null) {
            runContext.metric(Counter.of("num.dml.affected.rows", stats.getNumDmlAffectedRows(), tags));
        }

        if (stats.getTotalBytesBilled() != null) {
            runContext.metric(Counter.of("total.bytes.billed", stats.getTotalBytesBilled(), tags));
        }

        if (stats.getTotalBytesProcessed() != null) {
            runContext.metric(Counter.of("total.bytes.processed", stats.getTotalBytesProcessed(), tags));
        }

        if (stats.getTotalPartitionsProcessed() != null) {
            runContext.metric(Counter.of("total.partitions.processed", stats.getTotalPartitionsProcessed(), tags));
        }

        if (stats.getTotalSlotMs() != null) {
            runContext.metric(Counter.of("total.slot.ms", stats.getTotalSlotMs(), tags));
        }

        if (stats.getNumChildJobs() != null) {
            runContext.metric(Counter.of("num.child.jobs", stats.getNumChildJobs(), tags));
        }

        runContext
            .metric(Counter.of("cache.hit", stats.getCacheHit() ? 1 : 0, tags))
            .metric(Timer.of("duration", Duration.ofNanos(stats.getEndTime() - stats.getStartTime()), tags));
    }

    private List<Map<String, Object>> fetchResult(TableResult result) {
        return StreamSupport
            .stream(result.getValues().spliterator(), false)
            .map(fieldValues -> this.convertRows(result, fieldValues))
            .collect(Collectors.toList());
    }

    private Map<String, Object> convertRows(TableResult result, FieldValueList fieldValues) {
        HashMap<String, Object> row = new HashMap<>();
        result
            .getSchema()
            .getFields()
            .forEach(field -> {
                row.put(field.getName(), convertCell(field, fieldValues.get(field.getName()), false));
            });

        return row;
    }

    private Object convertCell(Field field, FieldValue value, boolean isRepeated) {
        if (field.getMode() == Field.Mode.REPEATED && !isRepeated) {
            return value
                .getRepeatedValue()
                .stream()
                .map(fieldValue -> this.convertCell(field, fieldValue, true))
                .collect(Collectors.toList());
        }

        if (value.isNull()) {
            return null;
        }

        if (LegacySQLTypeName.BOOLEAN.equals(field.getType())) {
            return value.getBooleanValue();
        }

        if (LegacySQLTypeName.BYTES.equals(field.getType())) {
            return value.getBytesValue();
        }

        if (LegacySQLTypeName.DATE.equals(field.getType())) {
            return LocalDate.parse(value.getStringValue());
        }

        if (LegacySQLTypeName.DATETIME.equals(field.getType())) {
            return Instant.parse(value.getStringValue() + "Z");
        }

        if (LegacySQLTypeName.FLOAT.equals(field.getType())) {
            return value.getDoubleValue();
        }

        if (LegacySQLTypeName.GEOGRAPHY.equals(field.getType())) {
            Pattern p = Pattern.compile("^POINT\\(([0-9.]+) ([0-9.]+)\\)$");
            Matcher m = p.matcher(value.getStringValue());

            if (m.find()) {
                return Arrays.asList(
                    Double.parseDouble(m.group(1)),
                    Double.parseDouble(m.group(2))
                );
            }

            throw new IllegalFormatFlagsException("Couldn't match '" + value.getStringValue() + "'");
        }

        if (LegacySQLTypeName.INTEGER.equals(field.getType())) {
            return value.getLongValue();
        }

        if (LegacySQLTypeName.NUMERIC.equals(field.getType())) {
            return value.getDoubleValue();
        }

        if (LegacySQLTypeName.RECORD.equals(field.getType())) {
            AtomicInteger counter = new AtomicInteger(0);

            return field
                .getSubFields()
                .stream()
                .map(sub -> new AbstractMap.SimpleEntry<>(
                    sub.getName(),
                    this.convertCell(sub, value.getRepeatedValue().get(counter.get()), false)
                ))
                .peek(u -> counter.getAndIncrement())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        if (LegacySQLTypeName.STRING.equals(field.getType())) {
            return value.getStringValue();
        }

        if (LegacySQLTypeName.TIME.equals(field.getType())) {
            return LocalTime.parse(value.getStringValue());
        }

        if (LegacySQLTypeName.TIMESTAMP.equals(field.getType())) {
            return Instant.ofEpochMilli(value.getTimestampValue() / 1000);
        }

        throw new IllegalArgumentException("Invalid type '" + field.getType() + "'");
    }

}
