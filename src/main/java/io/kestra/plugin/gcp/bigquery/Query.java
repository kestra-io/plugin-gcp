package io.kestra.plugin.gcp.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.*;
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
            title = "Create a table with a custom query.",
            full = true,
            code = """
                id: gcp_bq_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.gcp.bigquery.Query
                    destinationTable: "my_project.my_dataset.my_table"
                    writeDisposition: WRITE_APPEND
                    sql: |
                      SELECT
                        "hello" as string,
                        NULL AS `nullable`,
                        1 as int,
                        1.25 AS float,
                        DATE("2008-12-25") AS date,
                        DATETIME "2008-12-25 15:30:00.123456" AS datetime,
                        TIME(DATETIME "2008-12-25 15:30:00.123456") AS time,
                        TIMESTAMP("2008-12-25 15:30:00.123456") AS timestamp,
                        ST_GEOGPOINT(50.6833, 2.9) AS geopoint,
                        ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS `array`,
                        STRUCT(4 AS x, 0 AS y, ARRAY(SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS z) AS `struct`
                """
        ),
        @Example(
            title = "Execute a query and fetch results sets on another task.",
            full = true,
            code = """
                id: gcp_bq_query
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.gcp.bigquery.Query
                    fetch: true
                    sql: |
                      SELECT 1 as id, "John" as name
                      UNION ALL
                      SELECT 2 as id, "Doe" as name
                  - id: use_fetched_data
                    type: io.kestra.plugin.core.debug.Return
                    format: |
                      {% for row in outputs.fetch.rows %}
                      id : {{ row.id }}, name: {{ row.name }}
                      {% endfor %}
                """
        )
    },
    metrics = {
        @Metric(name = "cache.hit", type = Counter.TYPE, description= "Whether the query result was fetched from the query cache."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time it took for the query to run."),
        @Metric(name = "estimated.bytes.processed", type = Counter.TYPE, unit = "bytes", description = "The original estimate of bytes processed for the query."),
        @Metric(name = "total.bytes.billed", type = Counter.TYPE, unit = "bytes", description = "The total number of bytes billed for the query."),
        @Metric(name = "total.bytes.processed", type = Counter.TYPE, unit = "bytes", description = "The total number of bytes processed by the query."),
        @Metric(name = "total.partitions.processed", type = Counter.TYPE, unit = "partitions", description = "The totla number of partitions processed from all partitioned tables referenced in the job."),
        @Metric(name = "total.slot.ms", type = Counter.TYPE, description = "The slot-milliseconds consumed by the query."),
        @Metric(name = "num.dml.affected.rows", type = Counter.TYPE, unit = "records", description="The number of rows affected by a DML statement. Present only for DML statements INSERT, UPDATE or DELETE."),
        @Metric(name = "referenced.tables", type = Counter.TYPE, description="The number of tables referenced by the query."),
        @Metric(name = "num.child.jobs", type = Counter.TYPE, description="The number of child jobs executed by the query."),
    }
)
@Schema(
    title = "Run a SQL query in a specific BigQuery database."
)
@StoreFetchValidation
@StoreFetchDestinationValidation
public class Query extends AbstractJob implements RunnableTask<Query.Output>, QueryInterface {
    private Property<String> sql;

    @Builder.Default
    private Property<Boolean> legacySql = Property.of(false);

    @Builder.Default
    @Deprecated
    private boolean fetch = false;

    @Builder.Default
    @Deprecated
    private boolean store = false;

    @Builder.Default
    @Deprecated
    private boolean fetchOne = false;

    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.NONE);

    // private List<String> positionalParameters;

    // private Map<String, String> namedParameters;

    @Schema(
        title = "The clustering specification for the destination table."
    )
    private Property<List<String>> clusteringFields;

    @Schema(
        title = "[Experimental] Options allowing the schema of the destination table to be updated as a side effect of the query job.",
        description = " Schema update options are supported in two cases: " +
            "* when writeDisposition is WRITE_APPEND; \n" +
            "* when writeDisposition is WRITE_TRUNCATE and the destination" +
            " table is a partition of a table, specified by partition decorators. " +
            "\n" +
            " For normal tables, WRITE_TRUNCATE will always overwrite the schema."
    )
    @PluginProperty
    private Property<List<JobInfo.SchemaUpdateOption>> schemaUpdateOptions;

    @Schema(
        title = "The time partitioning field for the destination table."
    )
    private Property<String> timePartitioningField;

    @Schema(
        title = "The time partitioning type specification."
    )
    @Builder.Default
    private Property<TimePartitioning.Type> timePartitioningType = Property.of(TimePartitioning.Type.DAY);

    @Schema(
        title = "Range partitioning field for the destination table."
    )
    private Property<String> rangePartitioningField;

    @Schema(
        title = "The start of range partitioning, inclusive."
    )
    private Property<Long> rangePartitioningStart;

    @Schema(
        title = "The end range partitioning, inclusive."
    )
    private Property<Long> rangePartitioningEnd;

    @Schema(
        title = "The width of each interval."
    )
    private Property<Long> rangePartitioningInterval;

    @Schema(
        title = "Sets the default dataset.",
        description = "This dataset is used for all unqualified table names used in the query."
    )
    private Property<String> defaultDataset;

    @Schema(
        title = "Sets a priority for the query."
    )
    @Builder.Default
    private Property<QueryJobConfiguration.Priority> priority = Property.of(QueryJobConfiguration.Priority.INTERACTIVE);

    @Schema(
        title = "Sets whether the job is enabled to create arbitrarily large results.",
        description = "If `true` the query is allowed to create large results at a slight cost in performance. " +
            "`destinationTable` must be provided."
    )
    private Property<Boolean> allowLargeResults;

    @Schema(
        title = "Sets whether to look for the result in the query cache.",
        description = "The query cache is a best-effort cache that will be flushed whenever tables in the query are " +
            "modified. Moreover, the query cache is only available when `destinationTable` is not set "
    )
    private Property<Boolean> useQueryCache;

    @Schema(
        title = "Sets whether nested and repeated fields should be flattened.",
        description = "If set to `false`, allowLargeResults must be `true`."
    )
    @Builder.Default
    private Property<Boolean> flattenResults = Property.of(true);

    @Schema(
        title = "Sets whether to use BigQuery's legacy SQL dialect for this query.",
        description = " A valid query will return a mostly empty response with some processing statistics, " +
            "while an invalid query will return the same error it would if it wasn't a dry run."
    )
    @Builder.Default
    private Property<Boolean> useLegacySql = Property.of(false);

    @Schema(
        title = "Limits the billing tier for this job.",
        description = "Queries that have resource usage beyond this tier will fail (without incurring a charge). " +
            "If unspecified, this will be set to your project default."
    )
    private Property<Integer> maximumBillingTier;

    @Schema(
        title = "Limits the bytes billed for this job.",
        description = "Queries that will have bytes billed beyond this limit will fail (without incurring a charge). " +
            "If unspecified, this will be set to your project default."
    )
    private Property<Long> maximumBytesBilled;

    @Schema(
        title = "This is only supported in the fast query path.",
        description = "The maximum number of rows of data " +
            "to return per page of results. Setting this flag to a small value such as 1000 and then " +
            "paging through results might improve reliability when the query result set is large. In " +
            "addition to this limit, responses are also limited to 10 MB. By default, there is no maximum " +
            "row count, and only the byte limit applies."
    )
    private Property<Long> maxResults;

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
            runContext.render(this.dryRun).as(Boolean.class).orElseThrow(),
            runContext);
        JobStatistics.QueryStatistics queryJobStatistics = queryJob.getStatistics();

        QueryJobConfiguration config = queryJob.getConfiguration();
        TableId tableIdentity = config.getDestinationTable();

        if (tableIdentity != null) {
            logger.debug("Query loaded in: {}", tableIdentity.getDataset() + "." + tableIdentity.getTable());
        }

        FetchType fetchTypeRendered = this.computeFetchType(runContext);

        this.metrics(runContext, queryJobStatistics, queryJob, fetchTypeRendered);

        Output.OutputBuilder output = Output.builder()
            .jobId(queryJob.getJobId().getJob());


        if (!FetchType.NONE.equals(fetchTypeRendered)) {
            TableResult result = queryJob.getQueryResults();
            String[] tags = this.tags(queryJobStatistics, queryJob, fetchTypeRendered);

            runContext.metric(Counter.of("total.rows", result.getTotalRows(), tags));

            if (FetchType.STORE.equals(fetchTypeRendered)) {
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

                if (FetchType.FETCH.equals(fetchTypeRendered)) {
                    output.rows(fetch);
                } else {
                    output.row(fetch.size() > 0 ? fetch.get(0) : ImmutableMap.of());
                }
            }
        }

        if (tableIdentity != null) {
            DestinationTable destinationTable = new DestinationTable(tableIdentity.getProject(), tableIdentity.getDataset(), tableIdentity.getTable());
            output.destinationTable(destinationTable);
        }

        return output.build();
    }

    protected QueryJobConfiguration jobConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        String sql = runContext.render(this.sql).as(String.class).orElse(null);

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(sql)
            .setUseLegacySql(runContext.render(this.legacySql).as(Boolean.class).orElseThrow());

        if (this.clusteringFields != null) {
            builder.setClustering(Clustering.newBuilder().setFields(runContext.render(this.clusteringFields).asList(String.class)).build());
        }

        if (this.destinationTable != null) {
            builder.setDestinationTable(BigQueryService.tableId(runContext.render(this.destinationTable).as(String.class).orElseThrow()));
        }

        if (this.schemaUpdateOptions != null) {
            builder.setSchemaUpdateOptions(runContext.render(this.schemaUpdateOptions).asList(JobInfo.SchemaUpdateOption.class));
        }

        if (this.timePartitioningField != null) {
            builder.setTimePartitioning(TimePartitioning.newBuilder(runContext.render(this.timePartitioningType).as(TimePartitioning.Type.class).orElseThrow())
                .setField(runContext.render(this.timePartitioningField).as(String.class).orElseThrow())
                .build()
            );
        }

        if (this.rangePartitioningField != null) {
            builder.setRangePartitioning(RangePartitioning.newBuilder()
                .setField(runContext.render(this.rangePartitioningField).as(String.class).orElseThrow())
                .setRange(RangePartitioning.Range.newBuilder()
                    .setStart(runContext.render(this.rangePartitioningStart).as(Long.class).orElse(null))
                    .setEnd(runContext.render(this.rangePartitioningEnd).as(Long.class).orElse(null))
                    .setInterval(runContext.render(this.rangePartitioningInterval).as(Long.class).orElse(null))
                    .build()
                )
                .build()
            );
        }

        if (this.writeDisposition != null) {
            builder.setWriteDisposition(runContext.render(this.writeDisposition).as(JobInfo.WriteDisposition.class).orElseThrow());
        }

        if (this.createDisposition != null) {
            builder.setCreateDisposition(runContext.render(this.createDisposition).as(JobInfo.CreateDisposition.class).orElseThrow());
        }

        if (this.allowLargeResults != null) {
            builder.setAllowLargeResults(runContext.render(this.allowLargeResults).as(Boolean.class).orElseThrow());
        }

        if (this.useLegacySql != null) {
            builder.setUseLegacySql(runContext.render(this.useLegacySql).as(Boolean.class).orElseThrow());
        }

        if (this.jobTimeout != null) {
            builder.setJobTimeoutMs(runContext.render(this.jobTimeout).as(Duration.class).orElseThrow().toMillis());
        }

        if (this.maximumBillingTier != null) {
            builder.setMaximumBillingTier(runContext.render(this.maximumBillingTier).as(Integer.class).orElseThrow());
        }

        if (this.maximumBytesBilled != null) {
            builder.setMaximumBytesBilled(runContext.render(this.maximumBytesBilled).as(Long.class).orElseThrow());
        }

        if (this.maxResults != null) {
            builder.setMaxResults(runContext.render(this.maxResults).as(Long.class).orElseThrow());
        }

        if (this.priority != null) {
            builder.setPriority(runContext.render(this.priority).as(QueryJobConfiguration.Priority.class).orElseThrow());
        }

        if (this.useQueryCache != null) {
            builder.setUseQueryCache(runContext.render(this.useQueryCache).as(Boolean.class).orElseThrow());
        }

        if (this.dryRun != null) {
            builder.setDryRun(runContext.render(this.dryRun).as(Boolean.class).orElseThrow());
        }

        if (this.defaultDataset != null) {
            builder.setDefaultDataset(runContext.render(this.defaultDataset).as(String.class).orElseThrow());
        }

        if (this.flattenResults != null) {
            builder.setFlattenResults(runContext.render(this.flattenResults).as(Boolean.class).orElseThrow());
        }

        Map<String, String> finalLabels = new HashMap<>(BigQueryService.labels(runContext));
        var renderedLabels = runContext.render(this.labels).asMap(String.class, String.class);
        if (!renderedLabels.isEmpty()) {
            finalLabels.putAll(renderedLabels);
        }
        builder.setLabels(finalLabels);

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
            title = "The destination table (if one) or the temporary table created automatically "
        )
        private DestinationTable destinationTable;
    }

    private String[] tags(JobStatistics.QueryStatistics stats, Job queryJob, FetchType fetchType) {
        return new String[]{
            "statement_type", stats.getStatementType().name(),
            "fetch", FetchType.FETCH.equals(fetchType) || FetchType.FETCH_ONE.equals(fetchType) ? "true" : "false",
            "store", FetchType.STORE.equals(fetchType) ? "true" : "false",
            "project_id", queryJob.getJobId().getProject(),
            "location", queryJob.getJobId().getLocation(),
        };
    }

    public class DestinationTable {
        @Schema(
                title = "The project of the table"
        )
        private String project;
        @Schema(
                title = "The dataset of the table"
        )
        private String dataset;

        @Schema(
                title = "The table name"
        )
        private String table;

        public DestinationTable(String project, String dataset, String table){
            this.project = project;
            this.dataset = dataset;
            this.table = table;
        }

        public String getProject() {
            return project;
        }

        public String getDataset() {
            return dataset;
        }

        public String getTable() {
            return table;
        }
    }

    private void metrics(RunContext runContext, JobStatistics.QueryStatistics stats, Job queryJob, FetchType fetchTypeRendered) throws IllegalVariableEvaluationException {
        String[] tags = this.tags(stats, queryJob, fetchTypeRendered);

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
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (
            var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            Flux<Object> flowable = Flux
                .create(
                    s -> {
                        StreamSupport
                            .stream(result.iterateAll().spliterator(), false)
                            .forEach(fieldValues -> {
                                s.next(this.convertRows(result, fieldValues));
                            });

                        s.complete();
                    },
                    FluxSink.OverflowStrategy.BUFFER
                );
            Mono<Long> longMono = FileSerde.writeAll(output, flowable);

            // metrics & finalize
            Long lineCount = longMono.block();

            output.flush();

            return new AbstractMap.SimpleEntry<>(
                runContext.storage().putFile(tempFile),
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

        if (LegacySQLTypeName.BIGNUMERIC.equals(field.getType())) {
            return value.getNumericValue();
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
            return value.getTimestampInstant();
        }

        if (LegacySQLTypeName.JSON.equals(field.getType())) {
            try {
                return JacksonMapper.toMap(value.getStringValue());
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid data type [" + type + "] with value [" + value.getStringValue() + "]");
            }
        }

        if (LegacySQLTypeName.INTERVAL.equals(field.getType())) {
            return value.getStringValue();
        }

        if (LegacySQLTypeName.RANGE.equals(field.getType())) {
            if (LegacySQLTypeName.DATE.toString().equals(field.getRangeElementType().getType())) {
                return Map.of(
                    "start", LocalDate.parse(value.getRangeValue().getStart().getStringValue()),
                    "end", LocalDate.parse(value.getRangeValue().getEnd().getStringValue())
                );
            } else if (LegacySQLTypeName.DATETIME.toString().equals(field.getRangeElementType().getType())) {
                return Map.of(
                    "start", Instant.parse(value.getRangeValue().getStart().getStringValue() + "Z"),
                    "end", LocalDate.parse(value.getRangeValue().getEnd().getStringValue() + "Z")
                );
            } else if (LegacySQLTypeName.TIMESTAMP.toString().equals(field.getRangeElementType().getType())) {
                return Map.of(
                    "start", value.getRangeValue().getStart().getTimestampInstant(),
                    "end", value.getRangeValue().getEnd().getTimestampInstant()
                );
            } else {
                return value.getRangeValue().getValues();
            }
        }

        throw new IllegalArgumentException("Invalid type '" + field.getType() + "'");
    }

}