package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
public class StopTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;

    @Test
    void defaultsToWaitingWithATenMinuteTimeout() throws Exception {
        var stop = Stop.builder()
            .id("compute-stop-" + FriendlyId.createFriendlyId())
            .type(Stop.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, stop, ImmutableMap.of());

        assertThat(runContext.render(stop.getWaitUntilStopped()).as(Boolean.class).orElseThrow(), is(true));
        assertThat(runContext.render(stop.getTimeout()).as(Duration.class).orElseThrow(), is(Duration.ofMinutes(10)));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Disabled
    void run() throws Exception {
        var stop = Stop.builder()
            .id(Stop.class.getSimpleName())
            .type(Stop.class.getName())
            .projectId(Property.ofValue(project))
            .zone(Property.ofValue("us-central1-a"))
            .instanceName(Property.ofValue("my-batch-vm"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, stop, ImmutableMap.of());

        Stop.Output output = stop.run(runContext);
        assertThat(output.getStatus(), notNullValue());
    }
}
