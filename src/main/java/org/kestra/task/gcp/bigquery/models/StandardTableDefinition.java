package org.kestra.task.gcp.bigquery.models;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition.StreamingBuffer;
import com.google.cloud.bigquery.TimePartitioning;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

@Getter
@Builder
public class StandardTableDefinition {
    @OutputProperty(description = "Returns information on the table's streaming buffer if any exists. Returns {@code null} if no streaming buffer exists.")
    private final StreamingBuffer streamingBuffer;

    @OutputProperty(description = "Returns the clustering configuration for this table. If {@code null}, the table is not clustered.")
    private final Clustering clustering;

    @OutputProperty(description = "Returns the time partitioning configuration for this table. If {@code null}, the table is not time-partitioned.")
    private final TimePartitioning timePartitioning;

    @OutputProperty(description = "Returns the range partitioning configuration for this table. If {@code null}, the table is not range-partitioned.")
    private final RangePartitioning rangePartitioning;

    public static StandardTableDefinition of(com.google.cloud.bigquery.StandardTableDefinition standardTableDefinition) {
        return StandardTableDefinition.builder()
            .streamingBuffer(standardTableDefinition.getStreamingBuffer())
            .clustering(standardTableDefinition.getClustering() == null ? null : standardTableDefinition.getClustering())
            .timePartitioning(standardTableDefinition.getTimePartitioning() == null ? null : standardTableDefinition.getTimePartitioning())
            .rangePartitioning(standardTableDefinition.getRangePartitioning() == null ? null : standardTableDefinition.getRangePartitioning())
            .build();
    }
}
