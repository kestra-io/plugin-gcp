package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
    @PluginProperty(dynamic = false)
    private final com.google.cloud.bigquery.TimePartitioning.Type type;

    @Schema(title = "The number of milliseconds for which to keep the storage for a partition. When expired, the storage for the partition is reclaimed. If null, the partition does not expire.")
    @PluginProperty(dynamic = false)
    private final Duration expiration;

    @Schema(title = "If not set, the table is partitioned by pseudo column '_PARTITIONTIME'; if set, the table is partitioned by this field.")
    @PluginProperty(dynamic = true)
    private final String field;

    @Schema(title = "If set to true, queries over this table require a partition filter (that can be used for partition elimination) to be specified.")
    @PluginProperty(dynamic = false)
    private final Boolean requirePartitionFilter;

    public static TimePartitioning of(com.google.cloud.bigquery.TimePartitioning timePartitioning) {
        return TimePartitioning.builder()
            .type(timePartitioning.getType())
            .expiration(timePartitioning.getExpirationMs() == null ? null : Duration.ofMillis(timePartitioning.getExpirationMs()))
            .field(timePartitioning.getField())
            .requirePartitionFilter(timePartitioning.getRequirePartitionFilter())
            .build();
    }

    public com.google.cloud.bigquery.TimePartitioning to(RunContext runContext) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.TimePartitioning.Builder builder = com.google.cloud.bigquery.TimePartitioning.newBuilder(this.type);

        if (this.getExpiration() != null) {
            builder.setExpirationMs(this.getExpiration().toMillis());
        }

        if (this.getField() != null) {
            builder.setField(runContext.render(this.field));
        }

        if (this.getExpiration() != null) {
            builder.setRequirePartitionFilter(this.getRequirePartitionFilter());
        }

        return builder.build();
    }
}
