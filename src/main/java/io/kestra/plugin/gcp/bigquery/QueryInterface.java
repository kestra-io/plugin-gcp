package io.kestra.plugin.gcp.bigquery;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface QueryInterface {
    @Schema(
        title = "SQL query"
    )
    Property<String> getSql();

    @Schema(
        title = "Use legacy SQL",
        description = "Default false; when true, runs the query with the legacy dialect"
    )
    Property<Boolean> getLegacySql();

    @Schema(
        title = "Fetch output (deprecated)",
        description = "Deprecated. Use fetchType=FETCH instead."
    )
    @PluginProperty
    @Deprecated
    boolean isFetch();

    @Schema(
        title = "Store output (deprecated)",
        description = "Deprecated. Use fetchType=STORE instead."
    )
    @PluginProperty
    @Deprecated
    boolean isStore();

    @Schema(
        title = "Fetch one row (deprecated)",
        description = "Deprecated. Use fetchType=FETCH_ONE instead."
    )
    @PluginProperty
    @Deprecated
    boolean isFetchOne();

    @Schema(
        title = "Fetch type",
        description = "Controls result handling: FETCH_ONE, FETCH, STORE, or NONE."
    )
    Property<FetchType> getFetchType();

    default FetchType computeFetchType(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.isFetch()) return FetchType.FETCH;
        if (this.isStore()) return FetchType.STORE;
        if (this.isFetchOne()) return FetchType.FETCH_ONE;

        return runContext.render(this.getFetchType()).as(FetchType.class).orElseThrow();
    }
}
