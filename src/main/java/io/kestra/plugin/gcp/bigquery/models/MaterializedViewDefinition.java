package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.time.Duration;
import java.time.Instant;

@Getter
@Builder
@Jacksonized
public class MaterializedViewDefinition {
    @Schema(title = "Date when this materialized view was last modified")
    private final Instant lastRefreshDate;

    @Schema(title = "the query whose result is persisted")
    @PluginProperty(dynamic = true)
    public final String query;

    @Schema(title = "is enable automatic refresh of the materialized view when the base table is updated")
    @PluginProperty(dynamic = false)
    private final Boolean enableRefresh;

    @Schema(title = "the maximum frequency at which this materialized view will be refreshed")
    @PluginProperty(dynamic = false)
    private final Duration refreshInterval;

    public static MaterializedViewDefinition of(com.google.cloud.bigquery.MaterializedViewDefinition materializedViewDefinition) {
        return MaterializedViewDefinition.builder()
            .lastRefreshDate(materializedViewDefinition.getLastRefreshTime() == null ? null : Instant.ofEpochMilli(materializedViewDefinition.getLastRefreshTime()))
            .query(materializedViewDefinition.getQuery())
            .enableRefresh(materializedViewDefinition.getEnableRefresh())
            .refreshInterval(materializedViewDefinition.getRefreshIntervalMs() == null ? null : Duration.ofMillis(materializedViewDefinition.getRefreshIntervalMs()))
            .build();
    }

    public com.google.cloud.bigquery.MaterializedViewDefinition to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.MaterializedViewDefinition.Builder builder = com.google.cloud.bigquery.MaterializedViewDefinition.newBuilder(runContext.render(this.query));

        if (this.enableRefresh != null) {
            builder.setEnableRefresh(this.enableRefresh);
        }

        if (this.refreshInterval != null) {
            builder.setRefreshIntervalMs(this.refreshInterval.toMillis());
        }

        return builder.build();
    }
}
