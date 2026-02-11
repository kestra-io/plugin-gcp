package io.kestra.plugin.gcp.monitoring;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.*;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Query CPU utilization over the last 5 minutes",
            code = """
                id: gcp_monitoring_query
                namespace: company.team
                tasks:
                  - id: query
                    type: io.kestra.plugin.gcp.monitoring.Query
                    projectId: "my-project"
                    filter: metric.type="compute.googleapis.com/instance/cpu/utilization"
                    window: PT5M
                """
        )
    }
)
@Schema(
    title = "Query Cloud Monitoring time series",
    description = "Fetches time series matching the rendered `filter` over the `window` lookback (default 5 minutes) using Cloud Monitoring. Returns the raw series list and the total count. Supports custom project, service account, and scopes."
)
public class Query extends AbstractMonitoringTask implements RunnableTask<Query.Output> {
    @Schema(
        title = "Filter expression",
        description = "Cloud Monitoring filter string rendered before execution; required"
    )
    @NotNull
    private Property<String> filter;

    @Schema(
        title = "Query window",
        description = "Lookback duration for the time interval; defaults to 5 minutes"
    )
    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(5));

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFilter = runContext.render(this.filter).as(String.class).orElseThrow();

        try (MetricServiceClient client = this.connection(runContext)) {
            var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();

            var rWindow = runContext.render(this.window)
                .as(Duration.class)
                .orElse(Duration.ofMinutes(5));

            long now = Instant.now().getEpochSecond();

            var request = ListTimeSeriesRequest.newBuilder()
                .setName("projects/" + rProjectId)
                .setFilter(rFilter)
                .setInterval(TimeInterval.newBuilder()
                    .setStartTime(Timestamp.newBuilder().setSeconds(now - rWindow.getSeconds()))
                    .setEndTime(Timestamp.newBuilder().setSeconds(now)))
                .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
                .build();

            var printer = JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .includingDefaultValueFields();

            var series = StreamSupport.stream(client.listTimeSeries(request).iterateAll().spliterator(), false)
                .map(ts -> {
                    try {
                        return JacksonMapper.ofJson().readValue(printer.print(ts), new TypeReference<Map<String, Object>>() {});
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

            runContext.logger().info("Fetched {} time series", series.size());

            return Output.builder()
                .count(series.size())
                .series(series)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total number of time series fetched",
            description = "Count of time series returned by the Cloud Monitoring query"
        )
        private final Integer count;

        @Schema(
            title = "List of time series",
            description = "Raw Cloud Monitoring time series in JSON form"
        )
        private final List<Map<String, Object>> series;
    }
}
