package io.kestra.plugin.gcp.dataflow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Wait for a Dataflow job to reach a target terminal state and trigger a flow execution.",
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
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, DataflowConnectionInterface {

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

    @Schema(title = "Lookback window applied to job state transitions")
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

        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobNamePrefix = runContext.render(this.jobNamePrefix).as(String.class).orElse("");
        var rTargetState = runContext.render(this.targetState).as(JobState.class).orElse(JobState.JOB_STATE_DONE).name();
        var rLookback = runContext.render(this.lookback).as(Duration.class).orElse(this.interval);

        var end = Instant.now();
        var start = end.minus(rLookback);

        var dataflow = this.dataflowClient(runContext);

        Job mostRecentJob = null;
        Instant mostRecentTime = null;

        String pageToken = null;
        do {
            var listRequest = dataflow.projects().locations().jobs()
                .list(rProjectId, rLocation);
            if (pageToken != null) {
                listRequest.setPageToken(pageToken);
            }
            var listResponse = listRequest.execute();
            if (listResponse.getJobs() != null) {
                for (var job : listResponse.getJobs()) {
                    var jobName = job.getName();
                    var state = job.getCurrentState();
                    var stateTimeStr = job.getCurrentStateTime();

                    if (jobName != null && jobName.startsWith(rJobNamePrefix) && rTargetState.equals(state) && stateTimeStr != null) {
                        var stateTime = Instant.parse(stateTimeStr);
                        if (stateTime.isAfter(start) && stateTime.isBefore(end)) {
                            if (mostRecentTime == null || stateTime.isAfter(mostRecentTime)) {
                                mostRecentTime = stateTime;
                                mostRecentJob = job;
                            }
                        }
                    }
                }
            }
            pageToken = listResponse.getNextPageToken();
        } while (pageToken != null);

        if (mostRecentJob != null) {
            var output = Output.builder()
                .jobId(mostRecentJob.getId())
                .jobName(mostRecentJob.getName())
                .state(mostRecentJob.getCurrentState())
                .build();

            var execution = TriggerService.generateExecution(this, conditionContext, context, output);
            return Optional.of(execution);
        }

        return Optional.empty();
    }

    protected Dataflow dataflowClient(RunContext runContext) throws Exception {
        return DataflowService.dataflowClient(runContext, this);
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
