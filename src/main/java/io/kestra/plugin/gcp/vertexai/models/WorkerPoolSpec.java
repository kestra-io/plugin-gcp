package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import javax.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class WorkerPoolSpec {
    @Schema(
        title = " The custom container task."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    private ContainerSpec containerSpec;

    @Schema(
        title = "The specification of a single machine."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    private MachineSpec machineSpec;

    @Schema(
        title = "The specification of the disk."
    )
    @PluginProperty(dynamic = false)
    private DiscSpec discSpec;

    @Schema(
        title = "The specification of the disk."
    )
    @PluginProperty(dynamic = false)
    private Integer replicaCount;

    @Schema(
        title = "The python package specs."
    )
    @PluginProperty(dynamic = false)
    private PythonPackageSpec pythonPackageSpec;

    public com.google.cloud.aiplatform.v1.WorkerPoolSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.WorkerPoolSpec.Builder builder = com.google.cloud.aiplatform.v1.WorkerPoolSpec
            .newBuilder()
            .setContainerSpec(this.getContainerSpec().to(runContext))
            .setMachineSpec(this.machineSpec.to(runContext));

        if (this.getDiscSpec() != null) {
            builder.setDiskSpec(this.discSpec.to(runContext));
        }

        if (this.getReplicaCount() != null) {
            builder.setReplicaCount(this.replicaCount);
        }

        if (this.getPythonPackageSpec() != null) {
            builder.setPythonPackageSpec(this.getPythonPackageSpec().to(runContext));
        }

        return builder.build();
    }
}
