package io.kestra.plugin.gcp.spanner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.google.cloud.spanner.*;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for new changes in a Spanner database and trigger a flow execution",
    description = "Polls a Spanner Change Stream at the configured interval and triggers a downstream execution."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger on database changes via Change Streams",
            full = true,
            code = """
                id: spanner_change_trigger
                namespace: company.team

                triggers:
                  - id: on_changes
                    type: io.kestra.plugin.gcp.spanner.Trigger
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    changeStreamName: my-change-stream
                    interval: PT1M

                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "Change detected: {{ trigger.changeCount }} records modified"
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, SpannerConnectionInterface {

    private static final int HEARTBEAT_MILLISECONDS = 10000;

    @NotNull
    @Schema(title = "The GCP project ID")
    @PluginProperty(group = "connection")
    private Property<String> projectId;

    @Schema(title = "The GCP service account")
    @PluginProperty(secret = true, group = "connection")
    private Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate")
    @PluginProperty(secret = true, group = "advanced")
    private Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "advanced")
    private Property<List<String>> scopes;

    @NotNull
    @Schema(title = "The Spanner instance ID")
    @PluginProperty(group = "connection")
    private Property<String> instanceId;

    @NotNull
    @Schema(title = "The Spanner database ID")
    @PluginProperty(group = "connection")
    private Property<String> databaseId;

    @Schema(
        title = "Spanner emulator host (`host:port`), for local testing only",
        description = "When set, the trigger connects to a local Spanner emulator instead of the real Spanner service."
    )
    @PluginProperty(group = "advanced")
    private Property<String> emulatorHost;

    @NotNull
    @Schema(title = "The Change Stream name to poll")
    @PluginProperty(group = "source")
    private Property<String> changeStreamName;

    @Schema(
        title = "Lookback window applied to poll stream records",
        description = "Defaults to the polling interval if not specified."
    )
    @PluginProperty(group = "processing")
    private Property<Duration> lookback;

    @Builder.Default
    @Schema(title = "The interval between polls")
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rChangeStreamName = runContext.render(this.changeStreamName).as(String.class).orElseThrow();
        if (!rChangeStreamName.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid changeStreamName. Only alphanumeric characters and underscores are allowed.");
        }

        var rLookback = runContext.render(this.lookback).as(Duration.class).orElse(this.interval);

        var end = Instant.now();
        var start = end.minus(rLookback);

        var startTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(start.getEpochSecond(), start.getNano());
        var endTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(end.getEpochSecond(), end.getNano());

        var activeStartTimestamp = new com.google.cloud.Timestamp[]{ startTimestamp };
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var rowCount = 0L;
        var changeCount = 0L;

        try (var outputStream = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
            try (var spanner = SpannerService.spannerClient(runContext, this)) {
                var dbClient = spanner.getDatabaseClient(SpannerService.databaseId(runContext, this));

                List<String> partitionTokens = executeWithRetry(
                    startTimestamp,
                    startVal -> {
                        activeStartTimestamp[0] = startVal;
                        return discoverPartitionTokens(runContext, dbClient, rChangeStreamName, startVal, endTimestamp);
                    },
                    logger
                );

                logger.info("Discovered Spanner change stream partition tokens: {}", partitionTokens);

                for (var token : partitionTokens) {
                    var querySql = "SELECT * FROM READ_" + rChangeStreamName + "(" +
                        "start_timestamp => @startTimestamp, " +
                        "end_timestamp => @endTimestamp, " +
                        "partition_token => @partitionToken, " +
                        "heartbeat_milliseconds => " + HEARTBEAT_MILLISECONDS +
                        ")";

                    var result = executeWithRetry(
                        activeStartTimestamp[0],
                        startVal -> {
                            var stmtBuilder = Statement.newBuilder(querySql);
                            SpannerService.bindParameter(stmtBuilder, "startTimestamp", startVal);
                            SpannerService.bindParameter(stmtBuilder, "endTimestamp", endTimestamp);
                            SpannerService.bindParameter(stmtBuilder, "partitionToken", token);
                            return executeQueryAndStore(runContext, dbClient, stmtBuilder.build(), outputStream);
                        },
                        logger
                    );
                    rowCount += result.getKey();
                    changeCount += result.getValue();
                }
            }
        }

        if (rowCount == 0 || changeCount == 0) {
            Files.deleteIfExists(tempFile.toPath());
            return Optional.empty();
        }

        var storageUri = runContext.storage().putFile(tempFile);

        var output = Output.builder()
            .rowCount(rowCount)
            .changeCount(changeCount)
            .uri(storageUri)
            .build();

        var execution = TriggerService.generateExecution(this, conditionContext, context, output);
        return Optional.of(execution);
    }

    private List<String> discoverPartitionTokens(RunContext runContext, DatabaseClient dbClient, String changeStreamName, com.google.cloud.Timestamp startTimestamp, com.google.cloud.Timestamp endTimestamp) throws Exception {
        var tokens = new ArrayList<String>();
        var querySql = "SELECT * FROM READ_" + changeStreamName + "(" +
            "start_timestamp => @startTimestamp, " +
            "end_timestamp => @endTimestamp, " +
            "partition_token => NULL, " +
            "heartbeat_milliseconds => " + HEARTBEAT_MILLISECONDS +
            ")";

        var stmtBuilder = Statement.newBuilder(querySql);
        SpannerService.bindParameter(stmtBuilder, "startTimestamp", startTimestamp);
        SpannerService.bindParameter(stmtBuilder, "endTimestamp", endTimestamp);

        try (var resultSet = dbClient.singleUse().executeQuery(stmtBuilder.build())) {
            while (resultSet.next()) {
                if (!resultSet.isNull("ChangeRecord")) {
                    var changeRecords = resultSet.getStructList("ChangeRecord");
                    for (var record : changeRecords) {
                        if (!record.isNull("child_partitions_record")) {
                            var childPartitionsRecords = record.getStructList("child_partitions_record");
                            for (var cpRecord : childPartitionsRecords) {
                                if (!cpRecord.isNull("child_partitions")) {
                                    var childPartitions = cpRecord.getStructList("child_partitions");
                                    for (var cp : childPartitions) {
                                        if (!cp.isNull("token")) {
                                            tokens.add(cp.getString("token"));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return tokens;
    }

    private com.google.cloud.Timestamp parseEarliestTimestamp(SpannerException e, org.slf4j.Logger logger) {
        var msg = e.getMessage();
        var idx = msg.indexOf("earliest read timestamp:");
        if (idx != -1) {
            var tsStr = msg.substring(idx + "earliest read timestamp:".length()).trim();
            var spaceIdx = tsStr.indexOf(' ');
            if (spaceIdx != -1) {
                tsStr = tsStr.substring(0, spaceIdx);
            }
            tsStr = tsStr.trim();
            if (tsStr.endsWith(".")) {
                tsStr = tsStr.substring(0, tsStr.length() - 1);
            }
            try {
                var earliestInstant = Instant.parse(tsStr);
                var earliestTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(earliestInstant.getEpochSecond(), earliestInstant.getNano());
                logger.info("Provided startTimestamp was before stream creation. Retrying query with earliest read timestamp: {}", earliestTimestamp);
                return earliestTimestamp;
            } catch (Exception ex) {
                logger.error("Failed to parse earliest read timestamp: {}", tsStr, ex);
            }
        }
        return null;
    }

    private Map.Entry<Long, Long> executeQueryAndStore(RunContext runContext, DatabaseClient dbClient, Statement statement, BufferedOutputStream outputStream) throws Exception {
        var localRowCount = 0L;
        var localChangeCount = 0L;
        try (var resultSet = dbClient.singleUse().executeQuery(statement)) {
            while (resultSet.next()) {
                var rowMap = SpannerService.rowToMap(resultSet.getCurrentRowAsStruct());
                FileSerde.write(outputStream, rowMap);
                localRowCount++;

                if (!resultSet.isNull("ChangeRecord")) {
                    var changeRecords = resultSet.getStructList("ChangeRecord");
                    for (var record : changeRecords) {
                        if (!record.isNull("data_change_record")) {
                            localChangeCount += record.getStructList("data_change_record").size();
                        }
                    }
                }
            }
        }
        return new AbstractMap.SimpleEntry<>(localRowCount, localChangeCount);
    }

    @FunctionalInterface
    private interface SpannerQueryFunction<T> {
        T apply(com.google.cloud.Timestamp start) throws Exception;
    }

    private <T> T executeWithRetry(com.google.cloud.Timestamp start, SpannerQueryFunction<T> queryFunc, org.slf4j.Logger logger) throws Exception {
        try {
            return queryFunc.apply(start);
        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.OUT_OF_RANGE && e.getMessage().contains("earliest read timestamp")) {
                var earliestTimestamp = parseEarliestTimestamp(e, logger);
                if (earliestTimestamp != null) {
                    return queryFunc.apply(earliestTimestamp);
                }
            }
            throw e;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Total number of change records returned")
        private final Long rowCount;

        @Schema(title = "Total number of data modification changes detected")
        private final Long changeCount;

        @Schema(title = "The URI of stored change records")
        private final URI uri;
    }
}
