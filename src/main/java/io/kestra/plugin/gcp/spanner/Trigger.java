package io.kestra.plugin.gcp.spanner;

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
import io.kestra.plugin.gcp.GcpInterface;

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
    title = "Wait for new changes in a Spanner database and trigger a flow execution.",
    description = "Polls a Spanner Change Stream at the configured interval and triggers a downstream execution."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger on database changes via Change Streams.",
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
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, GcpInterface {

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
        title = "Spanner emulator host (`host:port`), for local testing only.",
        description = "When set, the trigger connects to a local Spanner emulator instead of the real Spanner service."
    )
    @PluginProperty(group = "advanced")
    private Property<String> emulatorHost;

    @NotNull
    @Schema(title = "The Change Stream name to poll")
    @PluginProperty(group = "source")
    private Property<String> changeStreamName;

    @Schema(
        title = "Lookback window window applied to poll stream records.",
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
        RunContext runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        String rChangeStreamName = runContext.render(this.changeStreamName).as(String.class).orElseThrow();
        Duration rLookback = runContext.render(this.lookback).as(Duration.class).orElse(this.interval);

        Instant end = Instant.now();
        Instant start = end.minus(rLookback);

        com.google.cloud.Timestamp startTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(start.getEpochSecond(), start.getNano());
        com.google.cloud.Timestamp endTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(end.getEpochSecond(), end.getNano());

        SpannerClientFactory clientFactory = SpannerClientFactory.builder()
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .impersonatedServiceAccount(this.impersonatedServiceAccount)
            .scopes(this.scopes)
            .instanceId(this.instanceId)
            .databaseId(this.databaseId)
            .emulatorHost(this.emulatorHost)
            .build();

        com.google.cloud.Timestamp finalStartTimestamp = startTimestamp;
        List<String> partitionTokens = new ArrayList<>();
        try {
            partitionTokens = discoverPartitionTokens(runContext, clientFactory, rChangeStreamName, finalStartTimestamp, endTimestamp);
        } catch (SpannerException e) {
            if (e.getErrorCode() == ErrorCode.OUT_OF_RANGE && e.getMessage().contains("earliest read timestamp")) {
                com.google.cloud.Timestamp earliestTimestamp = parseEarliestTimestamp(e, logger);
                if (earliestTimestamp != null) {
                    finalStartTimestamp = earliestTimestamp;
                    partitionTokens = discoverPartitionTokens(runContext, clientFactory, rChangeStreamName, finalStartTimestamp, endTimestamp);
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }

        logger.info("Discovered Spanner change stream partition tokens: {}", partitionTokens);

        List<Map<String, Object>> rows = new ArrayList<>();
        long changeCount = 0;

        for (String token : partitionTokens) {
            String querySql = "SELECT * FROM READ_" + rChangeStreamName + "(" +
                "start_timestamp => @startTimestamp, " +
                "end_timestamp => @endTimestamp, " +
                "partition_token => @partitionToken, " +
                "heartbeat_milliseconds => 10000" +
                ")";

            Statement.Builder stmtBuilder = Statement.newBuilder(querySql);
            clientFactory.bindParameter(stmtBuilder, "startTimestamp", finalStartTimestamp);
            clientFactory.bindParameter(stmtBuilder, "endTimestamp", endTimestamp);
            clientFactory.bindParameter(stmtBuilder, "partitionToken", token);

            try {
                changeCount += executeQuery(runContext, clientFactory, stmtBuilder.build(), rows);
            } catch (SpannerException e) {
                if (e.getErrorCode() == ErrorCode.OUT_OF_RANGE && e.getMessage().contains("earliest read timestamp")) {
                    com.google.cloud.Timestamp earliestTimestamp = parseEarliestTimestamp(e, logger);
                    if (earliestTimestamp != null) {
                        Statement.Builder retryBuilder = Statement.newBuilder(querySql);
                        clientFactory.bindParameter(retryBuilder, "startTimestamp", earliestTimestamp);
                        clientFactory.bindParameter(retryBuilder, "endTimestamp", endTimestamp);
                        clientFactory.bindParameter(retryBuilder, "partitionToken", token);
                        changeCount += executeQuery(runContext, clientFactory, retryBuilder.build(), rows);
                    } else {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }

        if (rows.isEmpty() || changeCount == 0) {
            return Optional.empty();
        }

        Output output = Output.builder()
            .rowCount((long) rows.size())
            .changeCount(changeCount)
            .rows(rows)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);
        return Optional.of(execution);
    }

    private List<String> discoverPartitionTokens(RunContext runContext, SpannerClientFactory clientFactory, String changeStreamName, com.google.cloud.Timestamp startTimestamp, com.google.cloud.Timestamp endTimestamp) throws Exception {
        List<String> tokens = new ArrayList<>();
        String querySql = "SELECT * FROM READ_" + changeStreamName + "(" +
            "start_timestamp => @startTimestamp, " +
            "end_timestamp => @endTimestamp, " +
            "partition_token => NULL, " +
            "heartbeat_milliseconds => 10000" +
            ")";

        Statement.Builder stmtBuilder = Statement.newBuilder(querySql);
        clientFactory.bindParameter(stmtBuilder, "startTimestamp", startTimestamp);
        clientFactory.bindParameter(stmtBuilder, "endTimestamp", endTimestamp);

        try (Spanner spanner = clientFactory.spannerClient(runContext)) {
            DatabaseClient dbClient = spanner.getDatabaseClient(clientFactory.databaseId(runContext));
            try (ResultSet resultSet = dbClient.singleUse().executeQuery(stmtBuilder.build())) {
                while (resultSet.next()) {
                    if (!resultSet.isNull("ChangeRecord")) {
                        List<Struct> changeRecords = resultSet.getStructList("ChangeRecord");
                        for (Struct record : changeRecords) {
                            if (!record.isNull("child_partitions_record")) {
                                List<Struct> childPartitionsRecords = record.getStructList("child_partitions_record");
                                for (Struct cpRecord : childPartitionsRecords) {
                                    if (!cpRecord.isNull("child_partitions")) {
                                        List<Struct> childPartitions = cpRecord.getStructList("child_partitions");
                                        for (Struct cp : childPartitions) {
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
        }
        return tokens;
    }

    private com.google.cloud.Timestamp parseEarliestTimestamp(SpannerException e, org.slf4j.Logger logger) {
        String msg = e.getMessage();
        int idx = msg.indexOf("earliest read timestamp:");
        if (idx != -1) {
            String tsStr = msg.substring(idx + "earliest read timestamp:".length()).trim();
            int spaceIdx = tsStr.indexOf(' ');
            if (spaceIdx != -1) {
                tsStr = tsStr.substring(0, spaceIdx);
            }
            tsStr = tsStr.trim();
            if (tsStr.endsWith(".")) {
                tsStr = tsStr.substring(0, tsStr.length() - 1);
            }
            try {
                Instant earliestInstant = Instant.parse(tsStr);
                com.google.cloud.Timestamp earliestTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(earliestInstant.getEpochSecond(), earliestInstant.getNano());
                logger.info("Provided startTimestamp was before stream creation. Retrying query with earliest read timestamp: {}", earliestTimestamp);
                return earliestTimestamp;
            } catch (Exception ex) {
                logger.error("Failed to parse earliest read timestamp: {}", tsStr, ex);
            }
        }
        return null;
    }


    private long executeQuery(RunContext runContext, SpannerClientFactory clientFactory, Statement statement, List<Map<String, Object>> rows) throws Exception {
        long changeCount = 0;
        try (Spanner spanner = clientFactory.spannerClient(runContext)) {
            DatabaseClient dbClient = spanner.getDatabaseClient(clientFactory.databaseId(runContext));
            try (ResultSet resultSet = dbClient.singleUse().executeQuery(statement)) {
                while (resultSet.next()) {
                    Map<String, Object> rowMap = clientFactory.rowToMap(resultSet.getCurrentRowAsStruct());
                    rows.add(rowMap);

                    if (!resultSet.isNull("ChangeRecord")) {
                        List<Struct> changeRecords = resultSet.getStructList("ChangeRecord");
                        runContext.logger().info("ChangeRecords size: {}", changeRecords.size());
                        for (Struct record : changeRecords) {
                            runContext.logger().info("record: data_change_record null? {}, heartbeat null? {}, child_partitions null? {}",
                                record.isNull("data_change_record"),
                                record.isNull("heartbeat_record"),
                                record.isNull("child_partitions_record"));
                            if (!record.isNull("data_change_record")) {
                                changeCount += record.getStructList("data_change_record").size();
                            }
                        }
                    }
                }
            }
        }
        return changeCount;
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class SpannerClientFactory extends AbstractSpanner {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Total number of change records returned")
        private final Long rowCount;

        @Schema(title = "Total number of data modification changes detected")
        private final Long changeCount;

        @Schema(title = "The retrieved change records")
        private final List<Map<String, Object>> rows;
    }
}
