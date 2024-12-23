package io.kestra.plugin.gcp.bigquery;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface QueryInterface {
    @Schema(
        title = "The sql query to run"
    )
    Property<String> getSql();

    @Schema(
        title = "Whether to use BigQuery's legacy SQL dialect for this query",
        description = "By default this property is set to false."
    )
    Property<Boolean> getLegacySql();

    @Schema(
        title = "Whether to Fetch the data from the query result to the task output. This is deprecated, use fetchType: FETCH instead"
    )
    @PluginProperty
    @Deprecated
    boolean isFetch();

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file. This is deprecated, use fetchType: STORE instead"
    )
    @PluginProperty
    @Deprecated
    boolean isStore();

    @Schema(
        title = "Whether to Fetch only one data row from the query result to the task output. This is deprecated, use fetchType: FETCH_ONE instead"
    )
    @PluginProperty
    @Deprecated
    boolean isFetchOne();

    @Schema(
        title = "Fetch type",
        description = """
            The way you want to store data :
              - FETCH_ONE - output the first row
              - FETCH - output all rows as output variable
              - STORE - store all rows to a file
              - NONE - do nothing
            """
    )
    Property<FetchType> getFetchType();

    default FetchType computeFetchType(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.isFetch()) return FetchType.FETCH;
        if (this.isStore()) return FetchType.STORE;
        if (this.isFetchOne()) return FetchType.FETCH_ONE;

        return runContext.render(this.getFetchType()).as(FetchType.class).orElseThrow();
    }
}
