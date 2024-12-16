package io.kestra.plugin.gcp.vertexai.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class GcsDestination {
    @Schema(
        title = "Google Cloud Storage URI to output directory.",
        description = "If the uri doesn't end with '/', a '/' will be automatically appended. The directory is created if it doesn't exist."
    )
    @NotNull
    private Property<String> outputUriPrefix;

    public com.google.cloud.aiplatform.v1.GcsDestination to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.aiplatform.v1.GcsDestination.Builder builder = com.google.cloud.aiplatform.v1.GcsDestination.newBuilder();

        if (this.getOutputUriPrefix() != null) {
            builder.setOutputUriPrefix(runContext.render(this.getOutputUriPrefix()).as(String.class).orElseThrow());
        }

        return builder.build();
    }
}
