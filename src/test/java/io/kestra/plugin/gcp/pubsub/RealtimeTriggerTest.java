package io.kestra.plugin.gcp.pubsub;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.FlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class RealtimeTriggerTest extends FlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void flow() throws Exception {
        String topicId = "test-topic-" + IdUtils.create();
        createTopic(topicId);
        var subscription = createSubscription(topicId);

        var task = Publish.builder()
            .id(Publish.class.getSimpleName())
            .type(Publish.class.getName())
            .serviceAccount(SERVICE_ACCOUNT)
            .topic(Property.ofValue(topicId))
            .projectId(Property.ofValue(this.project))
            .from(
                List.of(
                    Message.builder().data("Hello World").build()
                )
            )
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        var trigger = RealtimeTrigger.builder()
            .id("watch")
            .type(RealtimeTrigger.class.getName())
            .serviceAccount(SERVICE_ACCOUNT)
            .projectId(Property.ofValue(project))
            .subscription(Property.ofValue(subscription))
            .topic(Property.ofValue(topicId))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        try {
            Mono<Execution> executionMono = Mono.from(trigger.evaluate(context.getKey(), context.getValue()));

            Execution execution = executionMono.timeout(Duration.ofSeconds(30)).block();

            assertThat(execution, notNullValue());

            Map<String, Object> variables = execution.getTrigger().getVariables();
            assertThat(new String((byte[]) variables.get("data"), StandardCharsets.UTF_8), is("Hello World"));
        } finally {
            trigger.kill();
        }
    }

    private void createTopic(String topicId) throws Exception {
        try (var client = topicAdminClient()) {
            client.createTopic(ProjectTopicName.of(project, topicId));
        }
    }

    private String createSubscription(String topicId) throws Exception {
        String subId = "test-subscription-" + IdUtils.create();

        ProjectTopicName topicName = ProjectTopicName.of(project, topicId);
        ProjectSubscriptionName subName = ProjectSubscriptionName.of(project, subId);

        try (SubscriptionAdminClient subAdmin = subscriptionAdminClient()) {
            subAdmin.createSubscription(
                subName,
                topicName,
                PushConfig.getDefaultInstance(),
                10
            );
        }
        return subId;
    }
}
