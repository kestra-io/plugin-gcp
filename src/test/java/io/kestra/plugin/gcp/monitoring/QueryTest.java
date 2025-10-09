package io.kestra.plugin.gcp.monitoring;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest {
    private static final String PROJECT_ID = "kestra-unit-test";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(Property.ofValue(PROJECT_ID))
            .filter(Property.ofValue("metric.type=\"logging.googleapis.com/log_entry_count\""))
            .window(Property.ofValue(Duration.ofMinutes(10)))
            .build();

        var output = query.run(runContext);

        assertThat(output.getCount(), greaterThanOrEqualTo(0));
    }
}
