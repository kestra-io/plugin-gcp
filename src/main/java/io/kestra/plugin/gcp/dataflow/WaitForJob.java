package io.kestra.plugin.gcp.dataflow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.api.services.dataflow.model.Job;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    title = "Wait for a Dataflow job to complete",
    description = "Polls a Dataflow job status sequentially until it enters a terminal state (JOB_STATE_DONE, JOB_STATE_FAILED, JOB_STATE_CANCELLED, JOB_STATE_DRAINED, JOB_STATE_UPDATED)."
)
@Plugin(
    examples = {
        @Example(
            title = "Launch a template job and wait for its completion.",
            full = true,
            code = """
                id: dataflow_classic_template
                namespace: company.team

                inputs:
                  - id: input_path
                    type: STRING

                tasks:
                  - id: launch
                    type: io.kestra.plugin.gcp.dataflow.LaunchTemplate
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobName: my-etl-job
                    gcsPath: gs://my-bucket/templates/my-template
                    parameters:
                      inputFile: "{{ inputs.input_path }}"
                      outputTable: my-project:my_dataset.my_table

                  - id: wait
                    type: io.kestra.plugin.gcp.dataflow.WaitForJob
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobId: "{{ outputs.launch.jobId }}"
                    pollInterval: PT15S
                    maxDuration: PT1H
                """
        )
    }
)
public class WaitForJob extends AbstractDataflow implements RunnableTask<WaitForJob.Output> {

    @NotNull
    @Schema(title = "The Dataflow job ID")
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Builder.Default
    @Schema(title = "The interval between polls")
    @PluginProperty(group = "main")
    private Property<Duration> pollInterval = Property.ofValue(Duration.ofSeconds(15));

    @Builder.Default
    @Schema(title = "The maximum duration to wait before timing out")
    @PluginProperty(group = "main")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofHours(1));

    @ToString.Exclude
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    @ToString.Exclude
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        var rPollInterval = runContext.render(this.pollInterval).as(Duration.class).orElse(Duration.ofSeconds(15));
        var rMaxDuration = runContext.render(this.maxDuration).as(Duration.class).orElse(Duration.ofHours(1));

        var dataflow = this.dataflowClient(runContext);

        killable.set(() ->
        {
            try {
                var cancelJobPayload = new Job().setRequestedState("JOB_STATE_CANCELLED");
                dataflow.projects().locations().jobs()
                    .update(rProjectId, rLocation, rJobId, cancelJobPayload)
                    .execute();
                runContext.logger().info("Dataflow job '{}' successfully cancelled on task kill.", rJobId);
            } catch (Exception e) {
                runContext.logger().warn("Failed to cancel Dataflow job '{}' on task kill: {}", rJobId, e.getMessage());
            }
        });

        if (isKilled.get()) {
            throw new InterruptedException("Task was killed");
        }

        Job finalJob;
        try {
            finalJob = Await.until(
                () ->
                {
                    if (isKilled.get()) {
                        throw new RuntimeException("Task was killed");
                    }
                    try {
                        var job = dataflow.projects().locations().jobs()
                            .get(rProjectId, rLocation, rJobId)
                            .execute();
                        var state = job.getCurrentState();
                        if (isTerminalState(state)) {
                            return job;
                        }
                        return null;
                    } catch (java.io.IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                rPollInterval,
                rMaxDuration
            );
        } catch (TimeoutException e) {
            throw new TimeoutException("Dataflow job '" + rJobId + "' did not complete within the maximum duration of " + rMaxDuration);
        }

        var finalState = finalJob.getCurrentState();
        if ("JOB_STATE_FAILED".equals(finalState)) {
            throw new IllegalStateException("Dataflow job '" + rJobId + "' failed — check the Google Cloud Dataflow console for error details.");
        }
        if ("JOB_STATE_CANCELLED".equals(finalState)) {
            throw new IllegalStateException("Dataflow job '" + rJobId + "' was cancelled.");
        }

        var metricsMap = new HashMap<String, Object>();
        try {
            var metricsResponse = dataflow.projects().locations().jobs()
                .getMetrics(rProjectId, rLocation, rJobId)
                .execute();
            if (metricsResponse.getMetrics() != null) {
                for (var metric : metricsResponse.getMetrics()) {
                    if (metric.getName() != null && metric.getName().getName() != null && metric.getScalar() != null) {
                        metricsMap.put(metric.getName().getName(), metric.getScalar());
                    }
                }
            }
        } catch (java.io.IOException e) {
            runContext.logger().warn("Failed to retrieve metrics for Dataflow job '{}'", rJobId, e);
        }

        return Output.builder()
            .jobId(finalJob.getId())
            .state(finalState)
            .metrics(Map.copyOf(metricsMap))
            .build();
    }

    private boolean isTerminalState(String state) {
        return "JOB_STATE_DONE".equals(state) ||
            "JOB_STATE_FAILED".equals(state) ||
            "JOB_STATE_CANCELLED".equals(state) ||
            "JOB_STATE_DRAINED".equals(state) ||
            "JOB_STATE_UPDATED".equals(state);
    }

    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            var killAction = killable.get();
            if (killAction != null) {
                killAction.run();
            }
        }
    }

    @Override
    public void stop() {
        kill();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The job ID")
        private final String jobId;

        @Schema(title = "The final terminal state of the job")
        private final String state;

        @Schema(title = "A map containing job metrics at completion")
        private final Map<String, Object> metrics;
    }
}
