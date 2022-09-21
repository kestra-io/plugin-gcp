package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.ArrayUtils;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
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
@Plugin(
    examples = {
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
        ),
        @Example(
            full = true,
            title = "Execute a query and fetch results sets on another task",
            code = {
                "tasks:",
                "- id: fetch",
                "  type: io.kestra.plugin.gcp.bigquery.Query",
                "  fetch: true",
                "  sql: |",
                "    SELECT 1 as id, \"John\" as name",
                "    UNION ALL",
                "    SELECT 2 as id, \"Doe\" as name",
                "- id: use-fetched-data",
                "  type: io.kestra.core.tasks.debugs.Return",
                "  format: |",
                "    {{#each outputs.fetch.rows}}",
                "    id : {{ this.id }}, name: {{ this.name }}",
                "    {{/each}}"
            }
        )
    }
)
@Schema(
    title = "Execute BigQuery SQL query in a specific BigQuery database"
)
@StoreFetchValidation
@StoreFetchDestinationValidation
public class Query extends AbstractJob implements RunnableTask<Query.Output>, QueryInterface {
    private String sql;

    @Builder.Default
    private boolean legacySql = false;

    @Builder.Default
    private boolean fetch = false;

    @Builder.Default
    private boolean store = false;

    @Builder.Default
    private boolean fetchOne = false;

    // private List<String> positionalParameters;

    // private Map<String, String> namedParameters;

    @Schema(
        title = "The clustering specification for the destination table"
    )
    @PluginProperty(dynamic = true)
    private List<String> clusteringFields;

    @Schema(
        title = "[Experimental] Options allowing the schema of the destination table to be updated as a side effect of the query job",
        description = " Schema update options are supported in two cases: " +
            "* when writeDisposition is WRITE_APPEND; \n" +
            "* when writeDisposition is WRITE_TRUNCATE and the destination" +
            " table is a partition of a table, specified by partition decorators. " +
            "\n" +
            " For normal tables, WRITE_TRUNCATE will always overwrite the schema."
    )
    @PluginProperty(dynamic = false)
    private List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;

    @Schema(
        title = "The time partitioning field for the destination table."
    )
    @PluginProperty(dynamic = true)
    private String timePartitioningField;

    @Schema(
        title = "The time partitioning type specification."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

    @Schema(
        title = "Range partitioning field for the destination table."
    )
    @PluginProperty(dynamic = true)
    private String rangePartitioningField;

    @Schema(
        title = "The start of range partitioning, inclusive."
    )
    @PluginProperty(dynamic = true)
    private Long rangePartitioningStart;

    @Schema(
        title = "The end range partitioning, inclusive."
    )
    @PluginProperty(dynamic = true)
    private Long rangePartitioningEnd;

    @Schema(
        title = "The width of each interval."
    )
    @PluginProperty(dynamic = true)
    private Long rangePartitioningInterval;

    @Schema(
        title = "Sets the default dataset.",
        description = "This dataset is used for all unqualified table names used in the query."
    )
    @PluginProperty(dynamic = true)
    private String defaultDataset;

    @Schema(
        title = "Sets a priority for the query."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private QueryJobConfiguration.Priority priority = QueryJobConfiguration.Priority.INTERACTIVE;

    @Schema(
        title = "Sets whether the job is enabled to create arbitrarily large results.",
        description = "If `true` the query is allowed to create large results at a slight cost in performance. " +
            "`destinationTable` must be provide"
    )
    @PluginProperty(dynamic = false)
    private Boolean allowLargeResults;

    @Schema(
        title = "Sets whether to look for the result in the query cache.",
        description = "The query cache is a best-effort cache that will be flushed whenever tables in the query are " +
            "modified. Moreover, the query cache is only available when `destinationTable` is not set "
    )
    @PluginProperty(dynamic = false)
    private Boolean useQueryCache;

    @Schema(
        title = "Sets whether nested and repeated fields should be flattened.",
        description = "If set to `false`, allowLargeResults must be `true`"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean flattenResults = true;

    @Schema(
        title = "Sets whether to use BigQuery's legacy SQL dialect for this query.",
        description = " A valid query will return a mostly empty response with some processing statistics, " +
            "while an invalid query will return the same error it would if it wasn't a dry run."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean useLegacySql = false;

    @Schema(
        title = "Limits the billing tier for this job.",
        description = "Queries that have resource usage beyond this tier will fail (without incurring a charge). " +
            "If unspecified, this will be set to your project default."
    )
    @PluginProperty(dynamic = false)
    private Integer maximumBillingTier;

    @Schema(
        title = "Limits the bytes billed for this job.",
        description = "Queries that will have bytes billed beyond this limit will fail (without incurring a charge). " +
            "If unspecified, this will be set to your project default."
    )
    @PluginProperty(dynamic = false)
    private Long maximumBytesBilled;

    @Schema(
        title = "This is only supported in the fast query path.",
        description = "The maximum number of rows of data " +
            "to return per page of results. Setting this flag to a small value such as 1000 and then " +
            "paging through results might improve reliability when the query result set is large. In " +
            "addition to this limit, responses are also limited to 10 MB. By default, there is no maximum " +
            "row count, and only the byte limit applies."
    )
    @PluginProperty(dynamic = false)
    private Long maxResults;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        QueryJobConfiguration jobConfiguration = this.jobConfiguration(runContext);

        logger.debug("Starting query: {}", jobConfiguration.getQuery());

        Job queryJob = this.waitForJob(
            logger,
            () -> connection
                .create(JobInfo.newBuilder(jobConfiguration)
                    .setJobId(BigQueryService.jobId(runContext, this))
                    .build()
                ),
            this.dryRun
        );

        JobStatistics.QueryStatistics queryJobStatistics = queryJob.getStatistics();

        QueryJobConfiguration config = queryJob.getConfiguration();
        TableId tableIdentity = config.getDestinationTable();

        logger.info("Query loaded in: {}", tableIdentity.getDataset() + "." + tableIdentity.getTable());

        this.metrics(runContext, queryJobStatistics, queryJob);

        Output.OutputBuilder output = Output.builder()
            .jobId(queryJob.getJobId().getJob());

        if (this.fetch || this.fetchOne || this.store) {
            TableResult result = queryJob.getQueryResults();
            String[] tags = this.tags(queryJobStatistics, queryJob);

            runContext.metric(Counter.of("total.rows", result.getTotalRows(), tags));

            if (this.store) {
                Map.Entry<URI, Long> store = this.storeResult(result, runContext);

                runContext.metric(Counter.of("fetch.rows", store.getValue(), tags));
                output
                    .uri(store.getKey())
                    .size(store.getValue());

            } else {
                List<Map<String, Object>> fetch = this.fetchResult(result);

                if (result.getTotalRows() > fetch.size()) {
                    throw new IllegalStateException("Invalid fetch rows, got " + fetch.size() + ", expected " + result.getTotalRows());
                }

                runContext.metric(Counter.of("fetch.rows", fetch.size(), tags));
                output.size((long) fetch.size());

                if (this.fetch) {
                    output.rows(fetch);
                } else {
                    output.row(fetch.size() > 0 ? fetch.get(0) : ImmutableMap.of());
                }
            }
                HashMap<String,String> destinationTable = new HashMap<>();
                destinationTable.put("project",tableIdentity.getProject());
                destinationTable.put("dataset",tableIdentity.getProject());
                destinationTable.put("table",tableIdentity.getProject());
                output.destinationTable(destinationTable);
        }

        return output.build();
    }

    protected QueryJobConfiguration jobConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        String sql = runContext.render(this.sql);

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(sql)
            .setUseLegacySql(this.legacySql);

        if (this.clusteringFields != null) {
            builder.setClustering(Clustering.newBuilder().setFields(runContext.render(this.clusteringFields)).build());
        }

        if (this.destinationTable != null) {
            builder.setDestinationTable(BigQueryService.tableId(runContext.render(this.destinationTable)));
        }

        if (this.schemaUpdateOptions != null) {
            builder.setSchemaUpdateOptions(this.schemaUpdateOptions);
        }

        if (this.timePartitioningField != null) {
            builder.setTimePartitioning(TimePartitioning.newBuilder(this.timePartitioningType)
                .setField(runContext.render(this.timePartitioningField))
                .build()
            );
        }

        if (this.rangePartitioningField != null) {
            builder.setRangePartitioning(RangePartitioning.newBuilder()
                .setField(runContext.render(this.rangePartitioningField))
                .setRange(RangePartitioning.Range.newBuilder()
                    .setStart(this.rangePartitioningStart)
                    .setEnd(this.rangePartitioningEnd)
                    .setInterval(this.rangePartitioningInterval)
                    .build()
                )
                .build()
            );
        }

        if (this.writeDisposition != null) {
            builder.setWriteDisposition(this.writeDisposition);
        }

        if (this.createDisposition != null) {
            builder.setCreateDisposition(this.createDisposition);
        }

        if (this.allowLargeResults != null) {
            builder.setAllowLargeResults(this.allowLargeResults);
        }

        if (this.useLegacySql != null) {
            builder.setUseLegacySql(this.useLegacySql);
        }

        if (this.labels != null) {
            builder.setLabels(this.labels);
        }

        if (this.jobTimeout != null) {
            builder.setJobTimeoutMs(this.jobTimeout.toMillis());
        }

        if (this.maximumBillingTier != null) {
            builder.setMaximumBillingTier(this.maximumBillingTier);
        }

        if (this.maximumBytesBilled != null) {
            builder.setMaximumBytesBilled(this.maximumBytesBilled);
        }

        if (this.maxResults != null) {
            builder.setMaxResults(this.maxResults);
        }

        if (this.priority != null) {
            builder.setPriority(this.priority);
        }

        if (this.useQueryCache != null) {
            builder.setUseQueryCache(this.useQueryCache);
        }

        if (this.dryRun != null) {
            builder.setDryRun(this.dryRun);
        }

        if (this.defaultDataset != null) {
            builder.setDefaultDataset(runContext.render(this.defaultDataset));
        }

        if (this.flattenResults != null) {
            builder.setFlattenResults(this.flattenResults);
        }

        return builder.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The job id"
        )
        private String jobId;

        @Schema(
            title = "List containing the fetched data",
            description = "Only populated if 'fetch' parameter is set to true."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Map containing the first row of fetched data",
            description = "Only populated if 'fetchOne' parameter is set to true."
        )
        private Map<String, Object> row;

        @Schema(
            title = "The size of the rows fetch"
        )
        private Long size;

        @Schema(
            title = "The uri of store result",
            description = "Only populated if 'store' is set to true."
        )
        private URI uri;

        @Schema(
            title = "The informations where the data are stored"
        )
        private HashMap<String,String> destinationTable;
    }

    private String[] tags(JobStatistics.QueryStatistics stats, Job queryJob) {
        return new String[]{
            "statement_type", stats.getStatementType().name(),
            "fetch", this.fetch || this.fetchOne ? "true" : "false",
            "store", this.store ? "true" : "false",
            "project_id", queryJob.getJobId().getProject(),
            "location", queryJob.getJobId().getLocation(),
        };
    }

    private void metrics(RunContext runContext, JobStatistics.QueryStatistics stats, Job queryJob) throws IllegalVariableEvaluationException {
        String[] tags = this.tags(stats, queryJob);

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

        if (stats.getReferencedTables() != null) {
            runContext.metric(Counter.of("referenced.tables", stats.getReferencedTables().size(), tags));
        }


        if (stats.getTotalSlotMs() != null) {
            runContext.metric(Counter.of("total.slot.ms", stats.getTotalSlotMs(), tags));
        }

        if (stats.getNumChildJobs() != null) {
            runContext.metric(Counter.of("num.child.jobs", stats.getNumChildJobs(), tags));
        }

        if (stats.getCacheHit() != null) {
            runContext.metric(Counter.of("cache.hit", stats.getCacheHit() ? 1 : 0, tags));
        }

        runContext.metric(Timer.of("duration", Duration.ofMillis(stats.getEndTime() - stats.getStartTime()), tags));
    }

    private List<Map<String, Object>> fetchResult(TableResult result) {
        return StreamSupport
            .stream(result.iterateAll().spliterator(), false)
            .map(fieldValues -> this.convertRows(result, fieldValues))
            .collect(Collectors.toList());
    }

    private Map.Entry<URI, Long> storeResult(TableResult result, RunContext runContext) throws IOException {
        // temp file
        File tempFile = runContext.tempFile(".ion").toFile();

        try (
            OutputStream output = new FileOutputStream(tempFile);
        ) {
            Flowable<Object> flowable = Flowable
                .create(
                    s -> {
                        StreamSupport
                            .stream(result.iterateAll().spliterator(), false)
                            .forEach(fieldValues -> {
                                s.onNext(this.convertRows(result, fieldValues));
                            });

                        s.onComplete();
                    },
                    BackpressureStrategy.BUFFER
                )
                .doOnNext(row -> FileSerde.write(output, row));

            // metrics & finalize
            Single<Long> count = flowable.count();
            Long lineCount = count.blockingGet();

            output.flush();

            return new AbstractMap.SimpleEntry<>(
                runContext.putTempFile(tempFile),
                lineCount
            );
        }
    }

    private Map<String, Object> convertRows(TableResult result, FieldValueList fieldValues) {
        Map<String, Object> row = new LinkedHashMap<>();
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
                // https://bugs.openjdk.java.net/browse/JDK-8148463
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
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
