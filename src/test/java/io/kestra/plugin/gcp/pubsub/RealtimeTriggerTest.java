package io.kestra.plugin.gcp.pubsub;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@KestraTest
class RealtimeTriggerTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void flow() throws Exception {
        try (
            var connections = PubSubTestConnections.open(FLOCI_GCP.getEmulatorHost());
            var resources = PubSubTestUtils.forFloci(project, connections)
        ) {
            String topic = resources.createTopic();
            String subscription = resources.createSubscription(topic);

            var task = Publish.builder()
                .id(Publish.class.getSimpleName())
                .type(Publish.class.getName())
                .topic(Property.ofValue(topic))
                .projectId(Property.ofValue(project))
                .from(
                    List.of(
                        Message.builder()
                            .data("Hello World")
                            .attributes(Map.of("source", "realtime-trigger-test"))
                            .build()
                    )
                )
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            var trigger = RealtimeTrigger.builder()
                .id("watch")
                .type(RealtimeTrigger.class.getName())
                .projectId(Property.ofValue(project))
                .subscription(Property.ofValue(subscription))
                .topic(Property.ofValue(topic))
                .build();
            var triggerCleanup = resources.registerActiveResource(
                () -> assertTimeoutPreemptively(Duration.ofSeconds(10), trigger::kill)
            );

            Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Mono<Execution> executionMono = Mono.from(trigger.evaluate(context.getKey(), context.getValue()));

            Execution execution = executionMono.timeout(Duration.ofSeconds(30)).block();

            assertThat(execution, notNullValue());

            Map<String, Object> variables = execution.getTrigger().getVariables();
            assertThat(new String((byte[]) variables.get("data"), StandardCharsets.UTF_8), is("Hello World"));
            assertThat(variables.get("attributes"), is(Map.of("source", "realtime-trigger-test")));

            triggerCleanup.close();

            resources.assertSubscriptionAcknowledged(subscription);
        }
    }
}
