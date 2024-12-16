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

@Getter
@Builder
@Jacksonized
public class TimePartitioning {
    @Schema(title = "The time partitioning type.")
    private final Property<com.google.cloud.bigquery.TimePartitioning.Type> type;

    @Schema(title = "The number of milliseconds for which to keep the storage for a partition. When expired, the storage for the partition is reclaimed. If null, the partition does not expire.")
    private final Property<Duration> expiration;

    @Schema(title = "If not set, the table is partitioned by pseudo column '_PARTITIONTIME'; if set, the table is partitioned by this field.")
    private final Property<String> field;

    @Schema(title = "If set to true, queries over this table require a partition filter (that can be used for partition elimination) to be specified.")
    private final Property<Boolean> requirePartitionFilter;

    public static TimePartitioning.Output of(com.google.cloud.bigquery.TimePartitioning timePartitioning) {
        return TimePartitioning.Output.builder()
            .type(timePartitioning.getType())
            .expiration(timePartitioning.getExpirationMs() == null ? null : Duration.ofMillis(timePartitioning.getExpirationMs()))
            .field(timePartitioning.getField())
            .requirePartitionFilter(timePartitioning.getRequirePartitionFilter())
            .build();
    }

    public com.google.cloud.bigquery.TimePartitioning to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.TimePartitioning.Builder builder = com.google.cloud.bigquery.TimePartitioning.newBuilder(runContext.render(this.type).as(com.google.cloud.bigquery.TimePartitioning.Type.class).orElse(null));

        if (this.getExpiration() != null) {
            builder.setExpirationMs(runContext.render(this.getExpiration()).as(Duration.class).orElseThrow().toMillis());
        }

        if (this.getField() != null) {
            builder.setField(runContext.render(this.field).as(String.class).orElseThrow());
        }

        if (this.getExpiration() != null) {
            builder.setRequirePartitionFilter(runContext.render(this.getRequirePartitionFilter()).as(Boolean.class).orElseThrow());
        }

        return builder.build();
    }

    @Getter
    @Builder
    public static class Output {
        private final com.google.cloud.bigquery.TimePartitioning.Type type;
        private final Duration expiration;
        private final String field;
        private final Boolean requirePartitionFilter;
    }
}
