package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class Scheduling {
    @Schema(
        title = "The maximum job running time. The default is 7 days."
    )
    @NotNull
    private Property<Duration> timeOut;

    @Schema(
        title = "Restarts the entire CustomJob if a worker gets restarted.",
        description = "This feature can be used by distributed training jobs that are not resilient to workers leaving and joining a job."
    )
    @NotNull
    private Property<Boolean> restartJobOnWorkerRestart;

    public com.google.cloud.aiplatform.v1.Scheduling to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.Scheduling.Builder builder = com.google.cloud.aiplatform.v1.Scheduling.newBuilder();

        if (this.getTimeOut() != null) {
            var timeOutRendered = runContext.render(this.getTimeOut()).as(Duration.class).orElseThrow();
            builder.setTimeout(com.google.protobuf.Duration.newBuilder()
                .setSeconds(timeOutRendered.getSeconds())
                .setNanos(timeOutRendered.getNano()).build());
        }

        if (this.getRestartJobOnWorkerRestart() != null) {
            builder.setRestartJobOnWorkerRestart(runContext.render(this.getRestartJobOnWorkerRestart()).as(Boolean.class).orElseThrow());
        }

        return builder.build();
    }
}
