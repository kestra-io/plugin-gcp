package io.kestra.plugin.gcp.vertexai.models;

import com.google.cloud.aiplatform.v1.EnvVar;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class ContainerSpec {
    @Schema(
        title = "The URI of a container image in the Container Registry that is to be run on each worker replica.",
        description = "Must be on google container registry, example: `gcr.io/{{ project }}/{{ dir }}/{{ image }}:{{ tag }}`"
    )
    @NotNull
    private Property<String> imageUri;

    @Schema(
        title = "The command to be invoked when the container is started.",
        description = "It overrides the entrypoint instruction in Dockerfile when provided."
    )
    private Property<List<String>> commands;

    @Schema(
        title = "The arguments to be passed when starting the container."
    )
    private Property<List<String>> args;

    @Schema(
        title = "Environment variables to be passed to the container.",
        description = "Maximum limit is 100."
    )
    private Property<Map<String, String>> env;

    public com.google.cloud.aiplatform.v1.ContainerSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder = com.google.cloud.aiplatform.v1.ContainerSpec.newBuilder()
            .setImageUri(runContext.render(this.getImageUri()).as(String.class).orElseThrow())
            .addAllCommand(runContext.render(this.getCommands()).asList(String.class))
            .addAllArgs(runContext.render(this.getArgs()).asList(String.class));

        if (this.getEnv() != null) {
            builder.addAllEnv(runContext.render(this.getEnv()).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(e -> EnvVar.newBuilder()
                    .setName(e.getKey())
                    .setValue(e.getValue())
                    .build()
                )
                .toList()
            );
        }

        return builder.build();
    }
}
