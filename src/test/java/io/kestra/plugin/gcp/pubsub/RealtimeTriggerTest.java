package io.kestra.plugin.gcp.pubsub;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@KestraTest
@Testcontainers
class RealtimeTriggerTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void flow() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            String topic = null;
            String subscription = null;
            RealtimeTrigger trigger = null;

            try {
                topic = createTopic();
                subscription = createSubscription(topic);

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

                trigger = RealtimeTrigger.builder()
                    .id("watch")
                    .type(RealtimeTrigger.class.getName())
                    .projectId(Property.ofValue(project))
                    .subscription(Property.ofValue(subscription))
                    .topic(Property.ofValue(topic))
                    .build();

                Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
                Mono<Execution> executionMono = Mono.from(trigger.evaluate(context.getKey(), context.getValue()));

                Execution execution = executionMono.timeout(Duration.ofSeconds(30)).block();

                assertThat(execution, notNullValue());

                Map<String, Object> variables = execution.getTrigger().getVariables();
                assertThat(new String((byte[]) variables.get("data"), StandardCharsets.UTF_8), is("Hello World"));
                assertThat(variables.get("attributes"), is(Map.of("source", "realtime-trigger-test")));

                var activeTrigger = trigger;
                assertTimeoutPreemptively(Duration.ofSeconds(10), activeTrigger::kill);
                trigger = null;

                assertSubscriptionAcknowledged(subscription);
            } finally {
                try {
                    if (trigger != null) {
                        var activeTrigger = trigger;
                        assertTimeoutPreemptively(Duration.ofSeconds(10), activeTrigger::kill);
                    }
                } finally {
                    try {
                        if (subscription != null) {
                            deleteSubscription(subscription);
                        }
                    } finally {
                        if (topic != null) {
                            deleteTopic(topic);
                        }
                    }
                }
            }
        }
    }

    private String createTopic() throws Exception {
        String topicId = "test-topic-" + IdUtils.create();
        try (var topicAdmin = PubSubClientConnection.current().createTopicAdminClient()) {
            topicAdmin.createTopic(ProjectTopicName.of(project, topicId));
        }
        return topicId;
    }

    private String createSubscription(String topicId) throws Exception {
        String subscriptionId = "test-subscription-" + IdUtils.create();

        try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            subscriptionAdmin.createSubscription(
                ProjectSubscriptionName.of(project, subscriptionId),
                ProjectTopicName.of(project, topicId),
                PushConfig.getDefaultInstance(),
                10
            );
        }
        return subscriptionId;
    }

    private void assertSubscriptionAcknowledged(String subscriptionId) throws Exception {
        try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            var pendingMessages = subscriptionAdmin.pull(
                PullRequest.newBuilder()
                    .setSubscription(ProjectSubscriptionName.of(project, subscriptionId).toString())
                    .setMaxMessages(10)
                    .setReturnImmediately(true)
                    .build()
            );
            assertThat(pendingMessages.getReceivedMessagesList(), is(empty()));
        }
    }

    private void deleteSubscription(String subscriptionId) throws Exception {
        try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            subscriptionAdmin.deleteSubscription(ProjectSubscriptionName.of(project, subscriptionId));
        }
    }

    private void deleteTopic(String topicId) throws Exception {
        try (var topicAdmin = PubSubClientConnection.current().createTopicAdminClient()) {
            topicAdmin.deleteTopic(ProjectTopicName.of(project, topicId));
        }
    }
}
