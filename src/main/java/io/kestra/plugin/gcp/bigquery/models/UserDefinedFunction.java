package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Getter
@Builder
@Jacksonized
public class UserDefinedFunction {
    @Schema(
        name = "The type of user defined function."
    )
    private final Property<com.google.cloud.bigquery.UserDefinedFunction.Type> type;

    @Schema(
        name = "Type of UserDefinedFunction",
        description = "If `type` is UserDefinedFunction.Type.INLINE, this method returns a code blob.\n" +
            "If `type` is UserDefinedFunction.Type.FROM_URI, the method returns a Google Cloud Storage URI (e.g. gs://bucket/path)."
    )
    private final Property<String> content;

    public static UserDefinedFunction.Output of(com.google.cloud.bigquery.UserDefinedFunction userDefinedFunction) {
        return UserDefinedFunction.Output.builder()
            .type(userDefinedFunction.getType())
            .content(userDefinedFunction.getContent())
            .build();
    }

    public com.google.cloud.bigquery.UserDefinedFunction to(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.getType()).as(com.google.cloud.bigquery.UserDefinedFunction.Type.class).orElse(null)
            == com.google.cloud.bigquery.UserDefinedFunction.Type.FROM_URI ?
            com.google.cloud.bigquery.UserDefinedFunction.fromUri(runContext.render(content).as(String.class).orElse(null)) :
            com.google.cloud.bigquery.UserDefinedFunction.inline(runContext.render(content).as(String.class).orElse(null));
    }

    @Getter
    @Builder
    public static class Output {
        private com.google.cloud.bigquery.UserDefinedFunction.Type type;
        private String content;
    }
}
