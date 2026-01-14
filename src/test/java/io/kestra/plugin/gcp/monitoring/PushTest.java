package io.kestra.plugin.gcp.monitoring;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
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
                .metricType(Property.ofValue(METRIC_TYPE + "test_metric"))
                .labels(Property.ofValue(Map.of("test_id", IdUtils.create())))
                .value(Property.ofValue(42.0))
                .build()
        );

        var push = Push.builder()
            .projectId(Property.ofValue(PROJECT_ID))
            .metrics(Property.ofValue(metrics))
            .build();

        var output = push.run(runContext);

        assertThat(output.getCount(), is(1));
    }
}
