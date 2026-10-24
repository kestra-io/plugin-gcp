package io.kestra.plugin.gcp.pubsub;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class TriggerTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

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
                        Message.builder().data("Hello World").build(),
                        Message.builder().attributes(Map.of("key", "value")).build()
                    )
                )
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            var trigger = Trigger.builder()
                .id("watch")
                .type(Trigger.class.getName())
                .projectId(Property.ofValue(project))
                .subscription(Property.ofValue(subscription))
                .topic(Property.ofValue(topic))
                .interval(Duration.ofSeconds(10))
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

            URI consumedMessagesUri = URI.create(uri);
            resources.registerCleanup(
                () -> storageInterface.delete(TenantService.MAIN_TENANT, null, consumedMessagesUri)
            );
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

            // This checks the current backlog after ack; it does not prove long-term redelivery safety.
            resources.assertSubscriptionAcknowledged(subscription);
        }
    }

    private String messageData(Object data) {
        if (data instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Objects.toString(data, null);
    }
}
