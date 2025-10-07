package io.kestra.plugin.gcp.monitoring;

import com.google.monitoring.v3.MetricDescriptorName;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class TriggerTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void evaluate() throws Exception {
        var runContext = runContextFactory.of();

        var metrics = List.of(
            Push.MetricValue.builder()
                .metricType("custom.googleapis.com/kestra_unit_test/kestra_unit_test_metric")
                .value(99.9)
                .build()
        );

        var push = Push.builder()
            .projectId(Property.ofValue("kestra-unit-test"))
            .metrics(Property.ofValue(metrics))
            .build();

        var pushOutput = push.run(runContext);
        assertThat(pushOutput.getCount(), is(1));

        Thread.sleep(5000);

        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(TriggerTest.class.getName())
            .projectId(Property.ofValue("kestra-unit-test"))
            .filter(Property.ofValue(
                "metric.type=\"custom.googleapis.com/kestra_unit_test/kestra_unit_test_metric\""
            ))
            .window(Property.ofValue(java.time.Duration.ofMinutes(10)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = io.kestra.core.utils.TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        try (var client = push.connection(runContext)) {
            metrics.forEach(metricValue -> client.deleteMetricDescriptor(MetricDescriptorName.of("kestra-unit-test", metricValue.getMetricType())));
        }
    }
}
