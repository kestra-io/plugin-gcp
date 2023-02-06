package io.kestra.plugin.gcp.bigquery;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface QueryInterface {
    @Schema(
        title = "The sql query to run"
    )
    @PluginProperty(dynamic = true)
    String getSql();

    @Schema(
        title = "Whether to use BigQuery's legacy SQL dialect for this query",
        description = "By default this property is set to false."
    )
    @PluginProperty
    boolean isLegacySql();

    @Schema(
        title = "Whether to Fetch the data from the query result to the task output"
    )
    @PluginProperty
    boolean isFetch();

    @Schema(
        title = "Whether to store the data from the query result into an ion serialized data file"
    )
    @PluginProperty
    boolean isStore();

    @Schema(
        title = "Whether to Fetch only one data row from the query result to the task output"
    )
    @PluginProperty
    boolean isFetchOne();
}
