package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Getter
@Builder
@Jacksonized
public class ViewDefinition {
    @Schema(title = "The query whose result is persisted")
    @PluginProperty(dynamic = true)
    public final String query;

    @Schema(title = "User defined functions that can be used by query. Returns null if not set.")
    private final List<UserDefinedFunction> viewUserDefinedFunctions;

    public static ViewDefinition of(com.google.cloud.bigquery.ViewDefinition viewDefinition) {
        return ViewDefinition.builder()
            .viewUserDefinedFunctions(viewDefinition.getUserDefinedFunctions() == null ? null : viewDefinition.getUserDefinedFunctions()
                .stream()
                .map(UserDefinedFunction::of)
                .collect(Collectors.toList())
            )
            .query(viewDefinition.getQuery())
            .build();
    }

    public com.google.cloud.bigquery.ViewDefinition to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.ViewDefinition.Builder builder = com.google.cloud.bigquery.ViewDefinition.newBuilder(runContext.render(this.query));

        if (this.viewUserDefinedFunctions != null) {
            builder.setUserDefinedFunctions(viewUserDefinedFunctions
                .stream()
                .map(throwFunction(userDefinedFunction -> userDefinedFunction.to(runContext)))
                .collect(Collectors.toList())
            );
        }

        return builder.build();
    }
}
