package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    @Schema(title = "Date when this materialized view was last modified.")
    private final Instant lastRefreshDate;

    @Schema(title = "The query whose result is persisted.")
    public final Property<String> query;

    @Schema(title = "Whether automatic refresh is enabled for the materialized view when the base table is updated.")
    private final Property<Boolean> enableRefresh;

    @Schema(title = "The maximum frequency at which this materialized view will be refreshed.")
    private final Property<Duration> refreshInterval;

    public static MaterializedViewDefinition.Output of(com.google.cloud.bigquery.MaterializedViewDefinition materializedViewDefinition) {
        return MaterializedViewDefinition.Output.builder()
            .lastRefreshDate(materializedViewDefinition.getLastRefreshTime() == null ? null : Instant.ofEpochMilli(materializedViewDefinition.getLastRefreshTime()))
            .query(materializedViewDefinition.getQuery())
            .enableRefresh(materializedViewDefinition.getEnableRefresh())
            .refreshInterval(materializedViewDefinition.getRefreshIntervalMs() == null ? null : Duration.ofMillis(materializedViewDefinition.getRefreshIntervalMs()))
            .build();
    }

    public com.google.cloud.bigquery.MaterializedViewDefinition to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.MaterializedViewDefinition.Builder builder = com.google.cloud.bigquery.MaterializedViewDefinition.newBuilder(runContext.render(this.query).as(String.class).orElse(null));

        if (this.enableRefresh != null) {
            builder.setEnableRefresh(runContext.render(this.enableRefresh).as(Boolean.class).orElseThrow());
        }

        if (this.refreshInterval != null) {
            builder.setRefreshIntervalMs(runContext.render(this.refreshInterval).as(Duration.class).orElseThrow().toMillis());
        }

        return builder.build();
    }

    @Getter
    @Builder
    public static class Output {
        private final Instant lastRefreshDate;
        public final String query;
        private final Boolean enableRefresh;
        private final Duration refreshInterval;
    }
}
