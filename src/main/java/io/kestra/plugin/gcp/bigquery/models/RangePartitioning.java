package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    private final Property<String> field;

    @Schema(name = "The range of range partitioning.")
    private final Range range;

    public static RangePartitioning.Output of(com.google.cloud.bigquery.RangePartitioning rangePartitioning) {
        return RangePartitioning.Output.builder()
            .field(rangePartitioning.getField())
            .range(Range.of(rangePartitioning.getRange()))
            .build();
    }

    public com.google.cloud.bigquery.RangePartitioning to(RunContext runContext) throws IllegalVariableEvaluationException {
        return com.google.cloud.bigquery.RangePartitioning.newBuilder()
            .setField(runContext.render(this.getField()).as(String.class).orElse(null))
            .setRange(this.range.to(runContext))
            .build();
    }

    @Getter
    @Builder
    public static class Output {
        private final String field;
        private final Range.Output range;
    }

    @Getter
    @Builder
    @Jacksonized
    public static class Range {
        @Schema(
            name = "The start of range partitioning."
        )
        private final Property<Long> start;

        @Schema(
            name = "The end of range partitioning."
        )
        private final Property<Long> end;

        @Schema(
            name = "The width of each interval."
        )
        private final Property<Long> interval;

        public static Range.Output of(com.google.cloud.bigquery.RangePartitioning.Range range) {
            return Range.Output.builder()
                .start(range.getStart())
                .end(range.getEnd())
                .interval(range.getInterval())
                .build();
        }

        public com.google.cloud.bigquery.RangePartitioning.Range to(RunContext runContext) throws IllegalVariableEvaluationException {
            return com.google.cloud.bigquery.RangePartitioning.Range.newBuilder()
                .setStart(runContext.render(this.start).as(Long.class).orElseThrow())
                .setEnd(runContext.render(this.end).as(Long.class).orElseThrow())
                .setInterval(runContext.render(this.interval).as(Long.class).orElseThrow())
                .build();
        }

        @Getter
        @Builder
        public static class Output {
            private final Long start;
            private final Long end;
            private final Long interval;
        }
    }
}
