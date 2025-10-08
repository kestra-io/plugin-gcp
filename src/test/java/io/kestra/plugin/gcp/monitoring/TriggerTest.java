package io.kestra.plugin.gcp.monitoring;

import com.google.monitoring.v3.MetricDescriptorName;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class TriggerTest {
    private static final String PROJECT_ID = "kestra-unit-test";

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void evaluate() throws Exception {
        var runContext = runContextFactory.of();

        var metrics = List.of(
            Push.MetricValue.builder()
                .metricType(Property.ofValue("custom.googleapis.com/kestra_unit_test/trigger_test_metric"))
                .value(Property.ofValue(99.9))
                .build()
        );

        var push = Push.builder()
            .projectId(Property.ofValue(PROJECT_ID))
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
                "metric.type=\"custom.googleapis.com/kestra_unit_test/trigger_test_metric\""
            ))
            .window(Property.ofValue(java.time.Duration.ofMinutes(10)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = io.kestra.core.utils.TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
    }
}
