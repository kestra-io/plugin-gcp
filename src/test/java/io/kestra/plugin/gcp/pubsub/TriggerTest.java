package io.kestra.plugin.gcp.pubsub;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.project}")
    private String project;

    @Test
    void flow() throws Exception {
        var task = Publish.builder()
            .id(Publish.class.getSimpleName())
            .type(Publish.class.getName())
            .topic(Property.ofValue("test-topic"))
            .projectId(Property.ofValue(project))
            .from(
                List.of(
                    Message.builder().data("Hello World".getBytes()).build(),
                    Message.builder().attributes(Map.of("key", "value")).build()
                )
            )
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        var trigger = io.kestra.plugin.gcp.pubsub.Trigger.builder()
            .id("watch")
            .type(io.kestra.plugin.gcp.pubsub.Trigger.class.getName())
            .projectId(Property.ofValue(project))
            .subscription(Property.ofValue("test-subscription"))
            .topic(Property.ofValue("test-topic"))
            .interval(java.time.Duration.ofSeconds(10))
            .maxRecords(Property.ofValue(2))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        var count = (Integer) execution.get().getTrigger().getVariables().get("count");
        var uri = (String) execution.get().getTrigger().getVariables().get("uri");

        assertThat(count, is(2));
        assertThat(uri, is(notNullValue()));
    }
}
