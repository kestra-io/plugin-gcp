package io.kestra.plugin.gcp.monitoring;

import com.google.monitoring.v3.MetricDescriptorName;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class PushTest {
    private static final String PROJECT_ID = "kestra-unit-test";
    private static final String METRIC_TYPE = "custom.googleapis.com/" + IdUtils.create() + "/";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var metrics = List.of(
            Push.MetricValue.builder()
                .metricType(Property.ofValue(METRIC_TYPE + "metric_one"))
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
