package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import javax.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class ContainerSpec {
    @Schema(
        title = "The URI of a container image in the Container Registry that is to be run on each worker replica.",
        description = "Must be on google container registry, example: `gcr.io/{{ project }}/{{ dir }}/{{ image }}:{{ tag }}`"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String imageUri;

    @Schema(
        title = "The command to be invoked when the container is started.",
        description = "It overrides the entrypoint instruction in Dockerfile when provided."
    )
    @PluginProperty(dynamic = true)
    private List<String> commands;

    @Schema(
        title = "The arguments to be passed when starting the container."
    )
    @PluginProperty(dynamic = true)
    private List<String> args;

    public com.google.cloud.aiplatform.v1.ContainerSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.aiplatform.v1.ContainerSpec.newBuilder()
            .setImageUri(runContext.render(this.getImageUri()))
            .addAllCommand(this.getCommands() != null ? runContext.render(this.getCommands()) : List.of())
            .addAllArgs(this.getArgs() != null ? runContext.render(this.getArgs()) : List.of())
            .build();
    }
}
