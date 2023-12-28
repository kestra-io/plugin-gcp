package io.kestra.plugin.gcp.bigquery.models;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.StandardTableDefinition.StreamingBuffer;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Getter
@Builder
@Jacksonized
public class StandardTableDefinition {
    @Schema(title = "Returns information on the table's streaming buffer, if exists. Returns {@code null} if no streaming buffer exists.")
    private final StreamingBuffer streamingBuffer;

    @Schema(title = "Returns the clustering configuration for this table. If {@code null}, the table is not clustered.")
    private final List<String> clustering;

    @Schema(title = "Returns the time partitioning configuration for this table. If {@code null}, the table is not time-partitioned.")
    private final TimePartitioning timePartitioning;

    @Schema(title = "Returns the range partitioning configuration for this table. If {@code null}, the table is not range-partitioned.")
    private final RangePartitioning rangePartitioning;

    public static StandardTableDefinition of(com.google.cloud.bigquery.StandardTableDefinition standardTableDefinition) {
        return StandardTableDefinition.builder()
            .streamingBuffer(standardTableDefinition.getStreamingBuffer())
            .clustering(standardTableDefinition.getClustering() == null ? null : standardTableDefinition.getClustering().getFields())
            .timePartitioning(standardTableDefinition.getTimePartitioning() == null ? null : TimePartitioning.of(standardTableDefinition.getTimePartitioning()))
            .rangePartitioning(standardTableDefinition.getRangePartitioning() == null ? null : RangePartitioning.of(standardTableDefinition.getRangePartitioning()))
            .build();
    }

    public com.google.cloud.bigquery.TableDefinition to(RunContext runContext, io.kestra.plugin.gcp.bigquery.models.Schema schema) throws IllegalVariableEvaluationException {
        com.google.cloud.bigquery.StandardTableDefinition.Builder builder = com.google.cloud.bigquery.StandardTableDefinition.newBuilder();

        if (this.clustering != null) {
            builder.setClustering(Clustering.newBuilder().setFields(runContext.render(this.clustering)).build());
        }

        if (this.timePartitioning != null) {
            builder.setTimePartitioning(this.timePartitioning.to(runContext));
        }

        if (this.rangePartitioning != null) {
            builder.setRangePartitioning(this.rangePartitioning.to(runContext));
        }

        if (schema != null) {
            builder.setSchema(schema.to(runContext));
        }

        return builder.build();
    }
}
