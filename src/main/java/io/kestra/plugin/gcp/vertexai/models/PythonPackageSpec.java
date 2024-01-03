package io.kestra.plugin.gcp.vertexai.models;

import com.google.cloud.aiplatform.v1.EnvVar;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class PythonPackageSpec {
    @Schema(
        title = "The Google Cloud Storage location of the Python package files which are the training program and its dependent packages.",
        description = "The maximum number of package URIs is 100."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> packageUris;

    @Schema(
        title = "The Google Cloud Storage location of the Python package files which are the training program and its dependent packages.",
        description = "The maximum number of package URIs is 100."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private List<String> args;

    @Schema(
        title = "Environment variables to be passed to the python module.",
        description = "Maximum limit is 100."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Map<String, String> envs;

    public com.google.cloud.aiplatform.v1.PythonPackageSpec to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.PythonPackageSpec.Builder builder = com.google.cloud.aiplatform.v1.PythonPackageSpec.newBuilder();

        if (this.packageUris != null) {
            builder.addAllPackageUris(runContext.render(this.packageUris));
        }

        if (this.args != null) {
            builder.addAllArgs(runContext.render(this.args));
        }

        if (this.packageUris != null) {
            builder.addAllEnv(this.envs
                .entrySet()
                .stream()
                .map(throwFunction(e -> EnvVar.newBuilder()
                    .setName(runContext.render(e.getKey()))
                    .setValue(runContext.render(e.getValue()))
                    .build()
                ))
                .collect(Collectors.toList())
            );
        }

        return builder.build();
    }
}
