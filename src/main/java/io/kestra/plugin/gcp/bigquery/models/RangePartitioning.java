package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class RangePartitioning {
    @Schema(name = "The range partitioning field.")
    @PluginProperty(dynamic = true)
    private final String field;
    
    @Schema(name = "The range of range partitioning.")
    private final Range range;

    public static RangePartitioning of(com.google.cloud.bigquery.RangePartitioning rangePartitioning) {
        return RangePartitioning.builder()
            .field(rangePartitioning.getField())
            .range(Range.of(rangePartitioning.getRange()))
            .build();
    }

    public com.google.cloud.bigquery.RangePartitioning to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.RangePartitioning.newBuilder()
            .setField(runContext.render(this.getField()))
            .setRange(this.range.to(runContext))
            .build();
    }

    @Getter
    @Builder
    @Jacksonized
    public static class Range {
        @Schema(
            name = "The start of range partitioning."
        )
        @PluginProperty(dynamic = false)
        private final Long start;

        @Schema(
            name = "The end of range partitioning."
        )
        @PluginProperty(dynamic = false)
        private final Long end;

        @Schema(
            name = "The width of each interval."
        )
        @PluginProperty(dynamic = false)
        private final Long interval;

        public static Range of(com.google.cloud.bigquery.RangePartitioning.Range range) {
            return Range.builder()
                .start(range.getStart())
                .end(range.getEnd())
                .interval(range.getInterval())
                .build();
        }

        public com.google.cloud.bigquery.RangePartitioning.Range to(RunContext runContext) throws IllegalVariableEvaluationException {
            return com.google.cloud.bigquery.RangePartitioning.Range.newBuilder()
                .setStart(this.start)
                .setEnd(this.end)
                .setInterval(this.interval)
                .build();
        }
    }
}
