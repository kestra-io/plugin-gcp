package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class MachineSpec {
    @Schema(
        title = " The type of the machine.",
        description = "See the [list of machine types supported for" +
            "prediction](https://cloud.google.com/vertex-ai/docs/predictions/configure-compute#machine-types)\n" +
            "See the [list of machine types supported for custom " +
            "training](https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types)."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String machineType;

    @Schema(
        title = "The number of accelerators to attach to the machine."
    )
    @PluginProperty(dynamic = false)
    private Integer acceleratorCount;

    @Schema(
        title = "The type of accelerator(s) that may be attached to the machine."
    )
    @PluginProperty(dynamic = true)
    private com.google.cloud.aiplatform.v1.AcceleratorType acceleratorType;

    public com.google.cloud.aiplatform.v1.MachineSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.MachineSpec.Builder builder = com.google.cloud.aiplatform.v1.MachineSpec.newBuilder()
            .setMachineType(runContext.render(this.getMachineType()));

        if (this.getAcceleratorCount() != null) {
            builder.setAcceleratorCount(this.getAcceleratorCount());
        }

        if (this.getAcceleratorType() != null) {
            builder.setAcceleratorType(this.getAcceleratorType());
        }

        return builder.build();
    }
}
