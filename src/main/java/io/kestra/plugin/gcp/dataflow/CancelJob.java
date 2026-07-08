package io.kestra.plugin.gcp.dataflow;

import com.google.api.services.dataflow.model.Job;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
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
    title = "Cancel or drain a running Dataflow job",
    description = "Requests a transition to JOB_STATE_CANCELLED to cancel immediately, or JOB_STATE_DRAINING to allow streaming pipelines to process in-flight data before stopping."
)
@Plugin(
    examples = {
        @Example(
            title = "Cancel a Dataflow job.",
            full = true,
            code = """
                id: cancel_dataflow_job
                namespace: company.team

                tasks:
                  - id: cancel
                    type: io.kestra.plugin.gcp.dataflow.CancelJob
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobId: 2026-06-25_00_00_00-123456789
                    drain: false
                """
        )
    }
)
public class CancelJob extends AbstractDataflow implements RunnableTask<CancelJob.Output> {

    @NotNull
    @Schema(title = "The Dataflow job ID")
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Builder.Default
    @Schema(title = "Whether to drain the job instead of cancelling it", description = "Draining allows in-flight data processing to finish (only applicable to streaming jobs).")
    @PluginProperty(group = "main")
    private Property<Boolean> drain = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();
        var rDrain = runContext.render(this.drain).as(Boolean.class).orElse(false);

        var requestedState = rDrain ? "JOB_STATE_DRAINING" : "JOB_STATE_CANCELLED";

        var jobPayload = new Job()
            .setRequestedState(requestedState);

        var dataflow = this.dataflowClient(runContext);
        var updateRequest = dataflow.projects().locations().jobs()
            .update(rProjectId, rLocation, rJobId, jobPayload);

        var response = updateRequest.execute();

        return Output.builder()
            .jobId(response.getId())
            .currentState(response.getCurrentState())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The job ID")
        private final String jobId;

        @Schema(title = "The current state of the job")
        private final String currentState;
    }
}
