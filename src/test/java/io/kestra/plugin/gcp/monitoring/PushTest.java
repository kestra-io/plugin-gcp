package io.kestra.plugin.gcp.monitoring;

import com.google.monitoring.v3.MetricDescriptorName;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PushTest {
    private static final String PROJECT_ID = "kestra-unit-test";
    private static final String METRIC_TYPE = "custom.googleapis.com/kestra_unit_test/";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var metrics = List.of(
            Push.MetricValue.builder()
                .metricType(METRIC_TYPE + "metric_one")
                .value(42.0)
                .build(),
            Push.MetricValue.builder()
                .metricType(METRIC_TYPE + "metric_two")
                .value(123.45)
                .build()
        );

        var push = Push.builder()
            .projectId(Property.ofValue(PROJECT_ID))
            .metrics(Property.ofValue(metrics))
            .build();

        var output = push.run(runContext);

        assertThat(output.getCount(), is(2));

        // we clean the metrics pushed
        try (var client = push.connection(runContext)) {
            metrics.forEach(metricValue -> client.deleteMetricDescriptor(MetricDescriptorName.of(PROJECT_ID, metricValue.getMetricType())));
        }
    }
}
