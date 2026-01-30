package io.kestra.plugin.gcp.monitoring;

import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.*;
import com.google.protobuf.Timestamp;
import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push metrics to Cloud Monitoring",
    description = "Writes one or more data points to Cloud Monitoring. Supports GAUGE, DELTA, and CUMULATIVE kinds; default window 1 minute for interval-based kinds."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Push a custom metric to Cloud Monitoring",
            code = """
                id: gcp_monitoring_push
                namespace: company.team

                tasks:
                  - id: push_metric
                    type: io.kestra.plugin.gcp.monitoring.Push
                    projectId: "demo-project"
                    metrics:
                      - metricType: "custom.googleapis.com/demo/requests_count"
                        value: 42.0
                      - metricType: "custom.googleapis.com/demo/latency_ms"
                        value: 123.4
                """
        )
    }
)
public class Push extends AbstractMonitoringTask implements RunnableTask<Push.Output> {
    @Schema(
        title = "Metrics",
        description = "List of metric payloads, each with metricType, value, and optional labels/kind"
    )
    private Property<List<MetricValue>> metrics;

    @Schema(
        title = "Interval window",
        description = "Lookback duration used for DELTA/CUMULATIVE intervals; default 1 minute"
    )
    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(1));

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWindow = runContext.render(this.window).as(Duration.class).orElse(Duration.ofMinutes(1));
        var rMetrics = runContext.render(this.metrics).asList(MetricValue.class);

        try (MetricServiceClient client = this.connection(runContext)) {
            var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();

            var timeSeriesList = rMetrics.stream()
                .map(throwFunction(mv -> {
                    var rMetricType = runContext.render(mv.getMetricType()).as(String.class).orElseThrow();
                    var rValue = runContext.render(mv.getValue()).as(Double.class).orElseThrow();
                    var rMetricKind = runContext.render(mv.getMetricKind()).as(MetricKind.class).orElse(MetricKind.GAUGE);
                    var rLabels = runContext.render(mv.getLabels()).asMap(String.class, String.class);

                    var metric = Metric.newBuilder()
                        .setType(rMetricType)
                        .putAllLabels(rLabels)
                        .build();

                    var resource = MonitoredResource.newBuilder()
                        .setType("global")
                        .putLabels("project_id", rProjectId)
                        .build();

                    var now = Instant.now();
                    var interval = buildInterval(rMetricKind, rWindow, now);

                    var point = Point.newBuilder()
                        .setValue(TypedValue.newBuilder().setDoubleValue(rValue).build())
                        .setInterval(interval)
                        .build();

                    return TimeSeries.newBuilder()
                        .setMetric(metric)
                        .setResource(resource)
                        .addPoints(point)
                        .build();
                }))
                .toList();

            client.createTimeSeries(CreateTimeSeriesRequest.newBuilder()
                .setName("projects/" + rProjectId)
                .addAllTimeSeries(timeSeriesList)
                .build());

            runContext.logger().info("Successfully pushed {} metrics", timeSeriesList.size());

            return Output.builder()
                .count(timeSeriesList.size())
                .build();
        }
    }

    private TimeInterval buildInterval(MetricKind kind, Duration window, Instant now) {
        if (kind == MetricKind.GAUGE) {
            return TimeInterval.newBuilder()
                .setStartTime(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .setEndTime(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();
        } else {
            return TimeInterval.newBuilder()
                .setStartTime(Timestamp.newBuilder().setSeconds(now.minus(window).getEpochSecond()))
                .setEndTime(Timestamp.newBuilder().setSeconds(now.getEpochSecond()))
                .build();
        }
    }

    @Builder
    @Getter
    public static class MetricValue {
        @Schema(
            title = "Metric type",
            description = "Fully qualified metric type, e.g., custom.googleapis.com/demo/requests_count"
        )
        private final Property<String> metricType;

        @Schema(
            title = "Metric value",
            description = "Numeric value to push"
        )
        private Property<Double> value;

        @Builder.Default
        @Schema(
            title = "Metric kind",
            description = "GAUGE (default), CUMULATIVE, or DELTA"
        )
        private Property<MetricKind> metricKind = Property.ofValue(MetricKind.GAUGE);

        @Schema(
            title = "Labels",
            description = "Optional key/value labels"
        )
        private Property<Map<String, String>> labels;
    }

    public enum MetricKind {
        GAUGE,
        CUMULATIVE,
        DELTA
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total number of metrics pushed"
        )
        private final Integer count;
    }
}
