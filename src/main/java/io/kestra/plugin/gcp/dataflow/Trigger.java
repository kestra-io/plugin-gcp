package io.kestra.plugin.gcp.dataflow;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for a Dataflow job to reach a target terminal state and trigger a flow execution",
    description = "Polls the Dataflow service at a regular interval and triggers execution when a job matching the name prefix transitions to the target state. If multiple jobs match, the trigger returns the most recently completed one."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger workflow when a Dataflow job completes.",
            full = true,
            code = """
                id: dataflow_job_completion_trigger
                namespace: company.team

                triggers:
                  - id: on_job_done
                    type: io.kestra.plugin.gcp.dataflow.Trigger
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobNamePrefix: my-etl-
                    targetState: JOB_STATE_DONE
                    interval: PT2M

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "Dataflow job {{ trigger.jobId }} completed with state: {{ trigger.state }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, DataflowConnectionInterface, StatefulTriggerInterface {

    @NotNull
    @Schema(title = "The GCP project ID")
    @PluginProperty(group = "connection")
    private Property<String> projectId;

    @Schema(title = "The GCP service account")
    @PluginProperty(secret = true, group = "execution")
    private Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate")
    @PluginProperty(secret = true, group = "advanced")
    private Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "advanced")
    private Property<List<String>> scopes;

    @NotNull
    @Schema(title = "The regional endpoint (e.g. us-central1)")
    @PluginProperty(group = "connection")
    private Property<String> location;

    @NotNull
    @Schema(title = "The Dataflow job name prefix to match")
    @PluginProperty(group = "source")
    private Property<String> jobNamePrefix;

    @Builder.Default
    @Schema(title = "The target state to trigger on (e.g. JOB_STATE_DONE, JOB_STATE_FAILED)")
    @PluginProperty(group = "processing")
    private Property<JobState> targetState = Property.ofValue(JobState.JOB_STATE_DONE);

    @Schema(
        title = "Lookback window applied to job state transitions",
        description = "Defaults to the polling interval if not specified."
    )
    @PluginProperty(group = "processing")
    private Property<Duration> lookback;

    @Schema(
        title = "State key",
        description = "Override key used to persist trigger state; defaults to namespace/flow/id"
    )
    @PluginProperty(group = "connection")
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "Optional TTL for trigger state entries"
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Schema(
        title = "Which state change events to trigger on",
        description = "Can be CREATE, UPDATE or CREATE_OR_UPDATE."
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

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

        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobNamePrefix = runContext.render(this.jobNamePrefix).as(String.class).orElse("");
        var rTargetState = runContext.render(this.targetState).as(JobState.class).orElse(JobState.JOB_STATE_DONE).name();
        var rLookback = runContext.render(this.lookback).as(Duration.class).orElse(this.interval);
        var rOn = runContext.render(this.on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(this.stateKey).as(String.class).orElse(defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(this.stateTtl).as(Duration.class);

        var end = Instant.now();
        var start = end.minus(rLookback);

        var dataflow = this.dataflowClient(runContext);

        Map<String, Entry> state = readState(runContext, rStateKey, rStateTtl);

        Job targetJob = null;
        Instant targetJobTime = null;
        boolean stateUpdated = false;

        String pageToken = null;
        do {
            var listRequest = dataflow.projects().locations().jobs()
                .list(rProjectId, rLocation);

            // Optimize API call by listing only terminated/active jobs depending on targetState
            if (
                rTargetState.equals("JOB_STATE_DONE") || rTargetState.equals("JOB_STATE_FAILED") ||
                    rTargetState.equals("JOB_STATE_CANCELLED") || rTargetState.equals("JOB_STATE_DRAINED") ||
                    rTargetState.equals("JOB_STATE_UPDATED")
            ) {
                listRequest.setFilter("TERMINATED");
            } else if (
                rTargetState.equals("JOB_STATE_RUNNING") || rTargetState.equals("JOB_STATE_PENDING") ||
                    rTargetState.equals("JOB_STATE_QUEUED")
            ) {
                listRequest.setFilter("ACTIVE");
            }

            if (pageToken != null) {
                listRequest.setPageToken(pageToken);
            }
            var listResponse = listRequest.execute();
            if (listResponse.getJobs() != null) {
                for (var job : listResponse.getJobs()) {
                    var jobName = job.getName();
                    var currentState = job.getCurrentState();
                    var stateTimeStr = job.getCurrentStateTime();

                    if (jobName != null && jobName.startsWith(rJobNamePrefix) && rTargetState.equals(currentState) && stateTimeStr != null) {
                        var stateTime = Instant.parse(stateTimeStr);
                        if (stateTime.isAfter(start) && stateTime.isBefore(end)) {
                            var candidate = Entry.candidate(job.getId(), job.getCurrentState(), stateTime);
                            var update = computeAndUpdateState(state, candidate, rOn);
                            stateUpdated = true;
                            if (update.fire()) {
                                if (targetJobTime == null || stateTime.isAfter(targetJobTime)) {
                                    targetJobTime = stateTime;
                                    targetJob = job;
                                }
                            }
                        }
                    }
                }
            }
            pageToken = listResponse.getNextPageToken();
        } while (pageToken != null);

        if (stateUpdated) {
            writeState(runContext, rStateKey, state, rStateTtl);
        }

        if (targetJob != null) {
            var output = Output.builder()
                .jobId(targetJob.getId())
                .jobName(targetJob.getName())
                .state(targetJob.getCurrentState())
                .build();

            var execution = TriggerService.generateExecution(this, conditionContext, context, output);
            return Optional.of(execution);
        }

        return Optional.empty();
    }

    protected Dataflow dataflowClient(RunContext runContext) throws Exception {
        return AbstractDataflow.dataflowClient(runContext, this);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The matched job ID")
        private final String jobId;

        @Schema(title = "The matched job name")
        private final String jobName;

        @Schema(title = "The terminal state matched")
        private final String state;
    }
}
