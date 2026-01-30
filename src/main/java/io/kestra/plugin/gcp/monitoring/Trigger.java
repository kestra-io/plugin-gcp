package io.kestra.plugin.gcp.monitoring;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on Cloud Monitoring results",
    description = "Polls Cloud Monitoring every `interval` (default 60s) with the filter over `window` (default 5m). Starts a Flow execution when at least one time series is found and exposes `series` and `count` in the trigger output. Requires Monitoring access on the target project; supports custom service accounts and scopes."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger when a Cloud Monitoring query returns non-empty results",
            full = true,
            code = """
                id: gcp_monitoring_trigger
                namespace: company.team
                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.series }}"
                    tasks:
                      - id: log
                        type: io.kestra.plugin.core.log.Log
                        message: "Metric: {{ json(taskrun.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.gcp.monitoring.Trigger
                    interval: "PT1M"
                    projectId: "gcp-project-id"
                    filter: 'metric.type="compute.googleapis.com/instance/cpu/utilization" AND resource.type="gce_instance"'
                    window: PT5M
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output> {
    @Schema(
        title = "Polling interval",
        description = "Duration between query runs; defaults to 60 seconds"
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Project ID",
        description = "Google Cloud project that holds the monitored metrics"
    )
    private Property<String> projectId;

    @Schema(
        title = "Service account",
        description = "Optional service account email to authenticate the Monitoring client; falls back to application default credentials"
    )
    protected Property<String> serviceAccount;

    @Schema(
        title = "OAuth scopes",
        description = "Scopes used when building credentials; defaults to `https://www.googleapis.com/auth/cloud-platform`"
    )
    @Builder.Default
    protected Property<java.util.List<String>> scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    @NotNull
    @Schema(
        title = "Filter expression",
        description = "Cloud Monitoring filter string evaluated each poll; the trigger fires only when it returns at least one time series"
    )
    private Property<String> filter;

    @Schema(
        title = "Query window",
        description = "Lookback duration for the filter; defaults to 5 minutes"
    )
    @Builder.Default
    private Property<Duration> window = Property.ofValue(Duration.ofMinutes(5));

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        var output = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .projectId(this.projectId)
            .filter(this.filter)
            .window(this.window)
            .build()
            .run(runContext);

        logger.debug("Cloud Monitoring query returned {} series", output.getCount());

        if (output.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }
}
