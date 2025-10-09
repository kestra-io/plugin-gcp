package io.kestra.plugin.gcp.monitoring;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest {
    private static final String PROJECT_ID = "kestra-unit-test";
    private static final String METRIC_TYPE = "custom.googleapis.com/kestra_unit_test/test_metric";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var metric = Push.MetricValue.builder()
            .metricType(Property.ofValue(METRIC_TYPE))
            .value(Property.ofValue(123.45))
            .labels(Property.ofValue(Map.of("test_id", IdUtils.create())))
            .build();

        var push = Push.builder()
            .projectId(Property.ofValue(PROJECT_ID))
            .metrics(Property.ofValue(List.of(metric)))
            .build();

        var pushOutput = push.run(runContext);
        assertThat(pushOutput.getCount(), is(1));

        Thread.sleep(5000);

        var query = Query.builder()
            .projectId(Property.ofValue(PROJECT_ID))
            .filter(Property.ofValue("metric.type=\"" + METRIC_TYPE + "\""))
            .window(Property.ofValue(Duration.ofMinutes(10)))
            .build();

        var output = query.run(runContext);

        assertThat(output.getCount(), greaterThanOrEqualTo(0));
    }
}
