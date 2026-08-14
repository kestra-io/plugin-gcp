package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.ListTransferRunsRequest;
import com.google.cloud.bigquery.datatransfer.v1.StartManualTransferRunsRequest;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfig;
import com.google.cloud.bigquery.datatransfer.v1.TransferRun;
import com.google.cloud.bigquery.datatransfer.v1.TransferState;
import com.google.protobuf.util.Timestamps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a BigQuery Data Transfer Service run",
    description = """
        Starts a manual run of an existing BigQuery Data Transfer Service transfer config, or re-attaches \
        to a run that is already pending or running for that config so a retried execution never starts a duplicate run. \
        Optionally polls the run until it reaches a terminal state, on a wall-clock deadline."""
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a scheduled query transfer config and wait for it to complete.",
            full = true,
            code = """
                id: bigquery_data_transfer
                namespace: company.team

                tasks:
                  - id: trigger_transfer
                    type: io.kestra.plugin.gcp.bigquery.RunTransferConfig
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    transferConfigName: projects/my-project/locations/us/transferConfigs/615123456789012345
                    pollInterval: PT15S
                    maxDuration: PT1H
                    # Enterprise Edition: emit the destination as a data-lineage asset.
                    assets:
                      enableAuto: true
                """
        )
    },
    metrics = {
        @Metric(name = "reattached", type = Counter.TYPE, description = "Whether the task re-attached to an already in-flight transfer run (1) or started a new one (0)."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The wall-clock time spent waiting for the transfer run to reach a terminal state, recorded only when `wait` is `true`.")
    }
)
public class RunTransferConfig extends AbstractDataTransfer implements RunnableTask<RunTransferConfig.Output> {

    @NotNull
    @Schema(
        title = "The transfer config resource name",
        description = "Format: `projects/{project}/locations/{location}/transferConfigs/{config}`."
    )
    @PluginProperty(group = "main")
    private Property<String> transferConfigName;

    @Builder.Default
    @Schema(
        title = "Whether to re-attach to an already pending or running run of this config instead of starting a new one",
        description = """
            When `true` (default), the task first looks for a run of this config that is already \
            pending or running and adopts it, so a retried execution never starts a duplicate run. \
            By default any in-flight run is adopted regardless of its age; set `reattachMaxAge` to opt into \
            treating an old in-flight run as stale so a fresh run is started instead. \
            Set to `false` to always start a new run."""
    )
    @PluginProperty(group = "main")
    private Property<Boolean> reattach = Property.ofValue(true);

    @Schema(
        title = "The maximum age of an in-flight run that can still be re-attached to",
        description = """
            Used only when `reattach` is `true`. When set, an in-flight (pending or running) run whose \
            schedule time is older than `now - reattachMaxAge` is treated as stale and is NOT adopted -- a \
            fresh run is started instead. When unset (default), any in-flight run is adopted regardless of \
            its age. This is independent of `maxDuration`, which only bounds how long this task waits for a \
            run to reach a terminal state."""
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> reattachMaxAge;

    @Builder.Default
    @Schema(
        title = "Whether to wait until the transfer run reaches a terminal state",
        description = """
            When `false`, the task returns as soon as the run is triggered and does not emit a destination \
            asset, since completion is not confirmed."""
    )
    @PluginProperty(group = "main")
    private Property<Boolean> wait = Property.ofValue(true);

    @Builder.Default
    @Schema(title = "The interval between polls, used only when `wait` is `true`")
    @PluginProperty(group = "main")
    private Property<Duration> pollInterval = Property.ofValue(Duration.ofSeconds(15));

    @Builder.Default
    @Schema(title = "The maximum duration to wait before timing out, used only when `wait` is `true`")
    @PluginProperty(group = "main")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofHours(1));

    @ToString.Exclude
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (DataTransferServiceClient client = this.connection(runContext)) {
            return run(runContext, client);
        }
    }

    // Package-private seam so unit tests can drive the task's logic against a client wired to a
    // local WireMock server, without touching the connection lifecycle managed by run(RunContext).
    Output run(RunContext runContext, DataTransferServiceClient client) throws Exception {
        var rTransferConfigName = runContext.render(this.transferConfigName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`transferConfigName` is required"));
        var rWait = runContext.render(this.wait).as(Boolean.class).orElse(true);
        var rReattach = runContext.render(this.reattach).as(Boolean.class).orElse(true);
        var rReattachMaxAge = runContext.render(this.reattachMaxAge).as(Duration.class).orElse(null);
        var rPollInterval = runContext.render(this.pollInterval).as(Duration.class).orElse(Duration.ofSeconds(15));
        var rMaxDuration = runContext.render(this.maxDuration).as(Duration.class).orElse(Duration.ofHours(1));

        var logger = runContext.logger();

        if (isKilled.get()) {
            throw new InterruptedException("Task was killed");
        }

        var inFlightRun = rReattach ? findInFlightRun(client, rTransferConfigName, rReattachMaxAge) : null;
        var reattached = inFlightRun != null;
        runContext.metric(Counter.of("reattached", reattached ? 1 : 0));

        TransferRun run;
        if (reattached) {
            run = inFlightRun;
            logger.info("Re-attaching to in-flight transfer run '{}' instead of starting a new one", run.getName());
        } else {
            var startResponse = client.startManualTransferRuns(
                StartManualTransferRunsRequest.newBuilder()
                    .setParent(rTransferConfigName)
                    .setRequestedRunTime(Timestamps.fromMillis(Instant.now().toEpochMilli()))
                    .build()
            );
            var runsList = startResponse.getRunsList();
            if (runsList.isEmpty()) {
                throw new IllegalStateException(
                    "startManualTransferRuns started no run for transfer config '" + rTransferConfigName +
                        "' -- the config may be disabled or the requested run time is outside its window"
                );
            }
            run = runsList.getFirst();
            logger.info("Started transfer run '{}'", run.getName());
        }

        var config = client.getTransferConfig(rTransferConfigName);

        if (!rWait) {
            return output(run.getName(), run.getState().name(), reattached, config.getDestinationDatasetId(), rTransferConfigName);
        }

        var runName = run.getName();
        var waitStart = Instant.now();
        TransferRun finalRun;
        try {
            finalRun = Await.until(
                () ->
                {
                    if (isKilled.get()) {
                        throw new RuntimeException("Task was killed");
                    }
                    var polled = client.getTransferRun(runName);
                    return isTerminal(polled.getState()) ? polled : null;
                },
                rPollInterval,
                rMaxDuration
            );
        } catch (TimeoutException e) {
            var lastObserved = client.getTransferRun(runName);
            throw new TimeoutException(
                "Transfer run '" + runName + "' did not reach a terminal state within " + rMaxDuration +
                    " -- last observed state was " + lastObserved.getState()
            );
        }

        runContext.metric(Timer.of("duration", Duration.between(waitStart, Instant.now())));

        if (finalRun.getState() == TransferState.FAILED || finalRun.getState() == TransferState.CANCELLED) {
            throw new IllegalStateException(
                "Transfer run '" + runName + "' ended in state " + finalRun.getState() + ": " + finalRun.getErrorStatus().getMessage()
            );
        }

        emitDestinationAsset(runContext, config, rTransferConfigName);
        return output(finalRun.getName(), finalRun.getState().name(), reattached, config.getDestinationDatasetId(), rTransferConfigName);
    }

    private static TransferRun findInFlightRun(DataTransferServiceClient client, String configName, Duration reattachMaxAge) {
        // reattachMaxAge is opt-in and deliberately independent of maxDuration: maxDuration is also this
        // task's own wait budget, so using it as the staleness cutoff would make a run that is still
        // legitimately in-flight after a first attempt timed out look "stale" on the retry, causing a
        // duplicate run to be started -- defeating the whole purpose of reattach.
        var cutoff = reattachMaxAge == null ? null : Instant.now().minus(reattachMaxAge);

        TransferRun mostRecent = null;
        for (TransferRun candidate : client.listTransferRuns(
            ListTransferRunsRequest.newBuilder()
                .setParent(configName)
                .addStates(TransferState.PENDING)
                .addStates(TransferState.RUNNING)
                .build()
        ).iterateAll()) {
            if (cutoff != null) {
                var scheduleTime = Instant.ofEpochMilli(Timestamps.toMillis(candidate.getScheduleTime()));
                if (scheduleTime.isBefore(cutoff)) {
                    continue;
                }
            }
            if (mostRecent == null || Timestamps.compare(candidate.getScheduleTime(), mostRecent.getScheduleTime()) > 0) {
                mostRecent = candidate;
            }
        }
        return mostRecent;
    }

    private static boolean isTerminal(TransferState state) {
        return state == TransferState.SUCCEEDED || state == TransferState.FAILED || state == TransferState.CANCELLED;
    }

    // The Enterprise Edition asset types used for a table or dataset lineage node.
    private static final String TABLE_ASSET_TYPE = "io.kestra.plugin.ee.assets.Table";
    private static final String DATASET_ASSET_TYPE = "io.kestra.plugin.ee.assets.Dataset";

    // Emit the destination as a data-lineage asset: a Table when the scheduled query targets a literal
    // (non-templated) destination table, otherwise a Dataset since only the dataset is known.
    private static void emitDestinationAsset(RunContext runContext, TransferConfig config, String transferConfigName) {
        var dataset = config.getDestinationDatasetId();
        if (dataset == null || dataset.isBlank()) {
            return;
        }

        var projectId = parseSegment(transferConfigName, "projects");
        var location = parseSegment(transferConfigName, "locations");
        var table = destinationTable(config);

        var metadata = new LinkedHashMap<String, Object>();
        metadata.put("system", "bigquery");

        Custom asset;
        if (table != null) {
            if (projectId != null) {
                metadata.put("database", projectId);
            }
            metadata.put("schema", dataset);
            metadata.put("name", table);

            var idParts = Stream.of(projectId, dataset, table).filter(Objects::nonNull).toList();
            asset = Custom.builder()
                .id(String.join(".", idParts))
                .type(TABLE_ASSET_TYPE)
                .metadata(metadata)
                .build();
        } else {
            if (location != null) {
                metadata.put("location", location);
            }

            var idParts = Stream.of(projectId, dataset).filter(Objects::nonNull).toList();
            asset = Custom.builder()
                .id(String.join(".", idParts))
                .type(DATASET_ASSET_TYPE)
                .metadata(metadata)
                .build();
        }

        try {
            runContext.assets().emit(new AssetEmit(List.of(), List.of(asset)));
            runContext.logger().debug("Emitted destination asset '{}'", asset.getId());
        } catch (UnsupportedOperationException e) {
            // Asset emission is an Enterprise Edition feature, unsupported on OSS, so skip quietly.
            runContext.logger().debug("Asset emission is not supported in this edition, skipping");
        } catch (QueueException | IllegalVariableEvaluationException e) {
            runContext.logger().warn("Failed to emit destination asset for '{}': {}", transferConfigName, e.getMessage());
        }
    }

    // Extract the destination table only when the scheduled query targets a literal name.
    // A templated name (containing placeholders like {run_time}) is not a stable lineage node, so it is skipped.
    static String destinationTable(TransferConfig config) {
        var params = config.getParams();
        if (!params.containsFields("destination_table_name_template")) {
            return null;
        }
        var template = params.getFieldsOrThrow("destination_table_name_template").getStringValue();
        if (template.isBlank() || template.contains("{")) {
            return null;
        }
        return template;
    }

    private static Output output(String runName, String state, boolean reattached, String destinationDatasetId, String transferConfigName) {
        return Output.builder()
            .runName(runName)
            .state(state)
            .reattached(reattached)
            .destinationDatasetId(destinationDatasetId)
            .projectId(parseSegment(transferConfigName, "projects"))
            .location(parseSegment(transferConfigName, "locations"))
            .build();
    }

    private static String parseSegment(String resourceName, String key) {
        var parts = resourceName.split("/");
        for (int i = 0; i < parts.length - 1; i++) {
            if (parts[i].equals(key)) {
                return parts[i + 1];
            }
        }
        return null;
    }

    @Override
    public void kill() {
        // The BigQuery Data Transfer Service v1 client exposes no cancel-run API (deleteTransferRun only
        // removes run history, it does not stop a running job), so killing this task can only stop the
        // local wait -- the transfer run itself keeps running on Google's side until it reaches a terminal state.
        isKilled.set(true);
    }

    @Override
    public void stop() {
        kill();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The full resource name of the transfer run")
        private final String runName;

        @Schema(title = "The state of the transfer run, terminal when `wait` is `true`")
        private final String state;

        @Schema(title = "Whether an existing in-flight run was re-attached instead of starting a new one")
        private final boolean reattached;

        @Schema(title = "The destination dataset ID of the transfer config")
        private final String destinationDatasetId;

        @Schema(title = "The GCP project ID, parsed from the transfer config resource name")
        private final String projectId;

        @Schema(title = "The location, parsed from the transfer config resource name")
        private final String location;
    }
}
