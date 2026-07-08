package io.kestra.plugin.gcp.dataflow;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

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
    title = "Retrieve status and metadata of a Dataflow job",
    description = "Queries the Google Cloud Dataflow service to get the current execution state, metadata, and execution metrics of a job."
)
@Plugin(
    examples = {
        @Example(
            title = "Get Dataflow job details.",
            full = true,
            code = """
                id: get_dataflow_job
                namespace: company.team

                tasks:
                  - id: get_job
                    type: io.kestra.plugin.gcp.dataflow.GetJob
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    location: us-central1
                    jobId: 2026-06-25_00_00_00-123456789
                """
        )
    }
)
public class GetJob extends AbstractDataflow implements RunnableTask<GetJob.Output> {

    @NotNull
    @Schema(title = "The Dataflow job ID")
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rLocation = runContext.render(this.location).as(String.class).orElseThrow();
        var rJobId = runContext.render(this.jobId).as(String.class).orElseThrow();

        var dataflow = this.dataflowClient(runContext);
        var job = dataflow.projects().locations().jobs()
            .get(rProjectId, rLocation, rJobId)
            .execute();

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

        var createTime = job.getCreateTime() != null ? Instant.parse(job.getCreateTime()) : null;
        var currentStateTime = job.getCurrentStateTime() != null ? Instant.parse(job.getCurrentStateTime()) : null;

        return Output.builder()
            .jobId(job.getId())
            .currentState(job.getCurrentState())
            .currentStateTime(currentStateTime)
            .createTime(createTime)
            .type(job.getType())
            .metrics(Map.copyOf(metricsMap))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The job ID")
        private final String jobId;

        @Schema(title = "The current execution state")
        private final String currentState;

        @Schema(title = "The timestamp of when the current state was entered")
        private final Instant currentStateTime;

        @Schema(title = "The timestamp of when the job was created")
        private final Instant createTime;

        @Schema(title = "The job type (e.g. JOB_TYPE_BATCH, JOB_TYPE_STREAMING)")
        private final String type;

        @Schema(title = "A map containing job metrics")
        private final Map<String, Object> metrics;
    }
}
