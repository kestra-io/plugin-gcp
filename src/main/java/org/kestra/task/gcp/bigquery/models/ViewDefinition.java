package org.kestra.task.gcp.bigquery.models;

import com.google.cloud.bigquery.UserDefinedFunction;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

import java.time.Instant;
import java.util.List;

@Getter
@Builder
public class ViewDefinition {
    @OutputProperty(description = "The query whose result is persisted")
    public final String query;

    @OutputProperty(description = "User defined functions that can be used by query. Returns null if not set.")
    private final List<UserDefinedFunction> viewUserDefinedFunctions;

    public static ViewDefinition of(com.google.cloud.bigquery.ViewDefinition viewDefinition) {
        return ViewDefinition.builder()
            .viewUserDefinedFunctions(viewDefinition.getUserDefinedFunctions() == null ? null : viewDefinition.getUserDefinedFunctions())
            .query(viewDefinition.getQuery())
            .build();
    }
}
