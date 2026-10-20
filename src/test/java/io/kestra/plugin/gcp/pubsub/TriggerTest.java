package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
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
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Testcontainers
class TriggerTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void flow() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            String topic = null;
            String subscription = null;
            URI consumedMessagesUri = null;

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
                            Message.builder().data("Hello World".getBytes(StandardCharsets.UTF_8)).build(),
                            Message.builder().attributes(Map.of("key", "value")).build()
                        )
                    )
                    .build();

                task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

                var trigger = io.kestra.plugin.gcp.pubsub.Trigger.builder()
                    .id("watch")
                    .type(io.kestra.plugin.gcp.pubsub.Trigger.class.getName())
                    .projectId(Property.ofValue(project))
                    .subscription(Property.ofValue(subscription))
                    .topic(Property.ofValue(topic))
                    .interval(java.time.Duration.ofSeconds(10))
                    .maxRecords(Property.ofValue(2))
                    .build();

                Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
                var triggerRunContext = context.getKey().getRunContext();
                Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

                assertThat(execution.isPresent(), is(true));

                var variables = execution.get().getTrigger().getVariables();
                var count = (Integer) variables.get("count");
                var uri = (String) variables.get("uri");

                assertThat(count, is(2));
                assertThat(uri, is(notNullValue()));

                consumedMessagesUri = URI.create(uri);
                List<Message> consumedMessages;
                try (var messagesFile = triggerRunContext.storage().getFile(consumedMessagesUri)) {
                    consumedMessages = FileSerde.readAll(messagesFile, Message.class)
                        .collectList()
                        .block();
                }

                assertThat(consumedMessages, hasSize(2));
                var messageWithPayload = consumedMessages.stream()
                    .filter(message -> "Hello World".equals(messageData(message.getData())))
                    .toList();
                assertThat(messageWithPayload, hasSize(1));
                assertThat(messageWithPayload.getFirst().getAttributes(), anEmptyMap());

                var messageWithAttributes = consumedMessages.stream()
                    .filter(message -> Map.of("key", "value").equals(message.getAttributes()))
                    .toList();
                assertThat(messageWithAttributes, hasSize(1));
                assertThat(messageData(messageWithAttributes.getFirst().getData()), is(emptyString()));

                try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
                    var pendingMessages = subscriptionAdmin.pull(
                        PullRequest.newBuilder()
                            .setSubscription(ProjectSubscriptionName.of(project, subscription).toString())
                            .setMaxMessages(10)
                            .setReturnImmediately(true)
                            .build()
                    );
                    // This checks the current backlog after ack; it does not prove long-term redelivery safety.
                    assertThat(pendingMessages.getReceivedMessagesList(), is(empty()));
                }
            } finally {
                try {
                    if (subscription != null) {
                        deleteSubscription(subscription);
                    }
                } finally {
                    try {
                        if (topic != null) {
                            deleteTopic(topic);
                        }
                    } finally {
                        if (consumedMessagesUri != null) {
                            storageInterface.delete(TenantService.MAIN_TENANT, null, consumedMessagesUri);
                        }
                    }
                }
            }
        }
    }

    private String createSubscription(String topicId) throws Exception {
        String subId = "test-subscription-" + IdUtils.create();

        ProjectTopicName topicName = ProjectTopicName.of(project, topicId);
        ProjectSubscriptionName subName = ProjectSubscriptionName.of(project, subId);

        try (SubscriptionAdminClient subAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            subAdmin.createSubscription(
                subName,
                topicName,
                PushConfig.getDefaultInstance(),
                10
            );
        }
        return subId;
    }

    private void deleteSubscription(String subscription) throws Exception {
        try (SubscriptionAdminClient client = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            client.deleteSubscription(ProjectSubscriptionName.of(project, subscription));
        }
    }

    private void deleteTopic(String topic) throws IOException {
        try (TopicAdminClient client = PubSubClientConnection.current().createTopicAdminClient()) {
            client.deleteTopic(ProjectTopicName.of(project, topic));
        }
    }

    private String createTopic() throws Exception {
        String topicId = "test-topic-" + IdUtils.create();
        try (TopicAdminClient client = PubSubClientConnection.current().createTopicAdminClient()) {
            client.createTopic(ProjectTopicName.of(project, topicId));
        }
        return topicId;
    }

    private String messageData(Object data) {
        if (data instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Objects.toString(data, null);
    }
}
