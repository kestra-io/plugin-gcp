package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class UserDefinedFunction {
    @Schema(
        name = "the type of user defined function."
    )
    @PluginProperty(dynamic = false)
    private final com.google.cloud.bigquery.UserDefinedFunction.Type type;

    @Schema(
        name = "Type of UserDefinedFunction",
        description = "If `type` is UserDefinedFunction.Type.INLINE this method returns a code blob.\n" +
            "If `type` is UserDefinedFunction.Type.FROM_URI the method returns a Google Cloud Storage URI (e.g. gs://bucket/path)"
    )
    @PluginProperty(dynamic = true)
    private final String content;

    public static UserDefinedFunction of(com.google.cloud.bigquery.UserDefinedFunction userDefinedFunction) {
        return UserDefinedFunction.builder()
            .type(userDefinedFunction.getType())
            .content(userDefinedFunction.getContent())
            .build();
    }

    public com.google.cloud.bigquery.UserDefinedFunction to(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.getType() == com.google.cloud.bigquery.UserDefinedFunction.Type.FROM_URI
            ? com.google.cloud.bigquery.UserDefinedFunction.fromUri(runContext.render(content))
            : com.google.cloud.bigquery.UserDefinedFunction.inline(runContext.render(content));
    }
}
