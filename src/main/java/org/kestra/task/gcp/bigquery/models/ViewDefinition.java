package org.kestra.task.gcp.bigquery.models;

import com.google.cloud.bigquery.UserDefinedFunction;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class ViewDefinition {
    @Schema(title = "The query whose result is persisted")
    public final String query;

    @Schema(title = "User defined functions that can be used by query. Returns null if not set.")
    private final List<UserDefinedFunction> viewUserDefinedFunctions;

    public static ViewDefinition of(com.google.cloud.bigquery.ViewDefinition viewDefinition) {
        return ViewDefinition.builder()
            .viewUserDefinedFunctions(viewDefinition.getUserDefinedFunctions() == null ? null : viewDefinition.getUserDefinedFunctions())
            .query(viewDefinition.getQuery())
            .build();
    }
}
