package io.kestra.plugin.gcp.pubsub;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectTopicName;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PublishThenConsumeTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void runWithList() throws Exception {
        try (
            var connections = PubSubTestConnections.open(FLOCI_GCP.getEmulatorHost());
            var resources = PubSubTestUtils.forFloci(project, connections)
        ) {
            var runContext = runContextFactory.of();
            String topic = resources.createTopic();
            String subscription = resources.createSubscription(topic);

            var publish = Publish.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .from(
                    List.of(
                        Message.builder().data("Hello World").build(),
                        Message.builder().attributes(Map.of("key", "value")).build()
                    )
                )
                .build();

            var publishOutput = publish.run(runContext);
            assertThat(publishOutput.getMessagesCount(), is(2));

            var consumeContext = runContextFactory.of();
            var consume = Consume.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .subscription(Property.ofValue(subscription))
                .maxRecords(Property.ofValue(2))
                .build();

            var consumeOutput = consume.run(consumeContext);
            URI consumedMessagesUri = consumeOutput.getUri();
            resources.registerCleanup(
                () -> storageInterface.delete(TenantService.MAIN_TENANT, null, consumedMessagesUri)
            );
            assertThat(consumeOutput.getCount(), is(2));

            List<Message> consumedMessages;
            try (var messagesFile = consumeContext.storage().getFile(consumedMessagesUri)) {
                consumedMessages = FileSerde.readAll(messagesFile, Message.class)
                    .collectList()
                    .block();
            }

            assertThat(consumedMessages, hasSize(2));
            var messagesWithData = consumedMessages.stream()
                .filter(message -> "Hello World".equals(messageData(message.getData())))
                .toList();
            assertThat(messagesWithData, hasSize(1));
            assertThat(messageData(messagesWithData.getFirst().getData()), is("Hello World"));
            assertThat(messagesWithData.getFirst().getAttributes(), anEmptyMap());

            var messagesWithAttributes = consumedMessages.stream()
                .filter(message -> Map.of("key", "value").equals(message.getAttributes()))
                .toList();
            assertThat(messagesWithAttributes, hasSize(1));
            // Pub/Sub represents an omitted payload as an empty byte string on delivery.
            assertThat(messageData(messagesWithAttributes.getFirst().getData()), is(emptyString()));

            resources.assertSubscriptionAcknowledged(subscription);
        }
    }

    @Test
    void runWithJson() throws Exception {
        try (
            var connections = PubSubTestConnections.open(FLOCI_GCP.getEmulatorHost());
            var resources = PubSubTestUtils.forFloci(project, connections)
        ) {
            String topic = resources.createTopic();
            String subscription = resources.createSubscription(topic);

            var runContext = runContextFactory.of();

            var publish = Publish.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .serdeType(Property.ofValue(SerdeType.JSON))
                .from(
                    List.of(
                        Message.builder().data("""
                            {"hello": "world"}""").build()
                    )
                )
                .build();

            var publishOutput = publish.run(runContext);
            assertThat(publishOutput.getMessagesCount(), is(1));

            var consumeContext = runContextFactory.of();
            var consume = Consume.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .serdeType(Property.ofValue(SerdeType.JSON))
                .subscription(Property.ofValue(subscription))
                .maxRecords(Property.ofValue(1))
                .build();

            var consumeOutput = consume.run(consumeContext);
            URI consumedMessagesUri = consumeOutput.getUri();
            resources.registerCleanup(
                () -> storageInterface.delete(TenantService.MAIN_TENANT, null, consumedMessagesUri)
            );
            assertThat(consumeOutput.getCount(), is(1));

            List<Message> consumedMessages;
            try (var messagesFile = consumeContext.storage().getFile(consumedMessagesUri)) {
                consumedMessages = FileSerde.readAll(messagesFile, Message.class)
                    .collectList()
                    .block();
            }

            assertThat(consumedMessages, hasSize(1));
            assertThat(consumedMessages.getFirst().getData(), is(Map.of("hello", "world")));
            assertThat(consumedMessages.getFirst().getAttributes(), anEmptyMap());

            resources.assertSubscriptionAcknowledged(subscription);
        }
    }

    @Test
    void runWithFile() throws Exception {
        try (
            var connections = PubSubTestConnections.open(FLOCI_GCP.getEmulatorHost());
            var resources = PubSubTestUtils.forFloci(project, connections)
        ) {
            String topic = resources.createTopic();
            String subscription = resources.createSubscription(topic);
            URI sourceUri;

            var runContext = runContextFactory.of();
            sourceUri = createTestFile(runContext, resources);

            var publish = Publish.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .from(sourceUri.toString())
                .build();

            var publishOutput = publish.run(runContext);
            assertThat(publishOutput.getMessagesCount(), is(2));

            var consumeContext = runContextFactory.of();
            var consume = Consume.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .subscription(Property.ofValue(subscription))
                .maxRecords(Property.ofValue(2))
                .build();

            var consumeOutput = consume.run(consumeContext);
            URI consumedMessagesUri = consumeOutput.getUri();
            resources.registerCleanup(
                () -> storageInterface.delete(TenantService.MAIN_TENANT, null, consumedMessagesUri)
            );
            assertThat(consumeOutput.getCount(), is(2));

            List<Message> consumedMessages;
            try (var messagesFile = consumeContext.storage().getFile(consumedMessagesUri)) {
                consumedMessages = FileSerde.readAll(messagesFile, Message.class)
                    .collectList()
                    .block();
            }

            assertThat(consumedMessages, hasSize(2));
            var messageWithPayload = consumedMessages.stream()
                .filter(message -> "Hello World".equals(messageData(message.getData())))
                .toList();
            assertThat(
                "Received messages: " + consumedMessages.stream()
                    .map(message -> "data=" + messageData(message.getData()) + ", attributes=" + message.getAttributes())
                    .toList(),
                messageWithPayload,
                hasSize(1)
            );
            assertThat(messageWithPayload.getFirst().getAttributes(), anEmptyMap());

            var messageWithAttributes = consumedMessages.stream()
                .filter(message -> Map.of("key", "value").equals(message.getAttributes()))
                .toList();
            assertThat(messageWithAttributes, hasSize(1));
            assertThat(messageData(messageWithAttributes.getFirst().getData()), is(emptyString()));

            resources.assertSubscriptionAcknowledged(subscription);
        }
    }

    // Floci 0.11.0 does not faithfully emulate ordered-subscription delivery semantics.
    @Test
    @EnabledIfEnvironmentVariable(
        named = "GOOGLE_APPLICATION_CREDENTIALS",
        matches = ".+",
        disabledReason = "Floci 0.11.0 does not faithfully emulate ordered-subscription delivery semantics."
    )
    void runWithMessagesWithOrderingKey() throws Exception {
        try (var resources = PubSubTestUtils.withDefaultCredentials(project)) {
            String topic = resources.createTopic();
            String subscription = resources.createSubscription(topic);
            var runContext = runContextFactory.of();
            var publish = Publish.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .from(
                    List.of(
                        Message.builder()
                            .data("first message")
                            .orderingKey("key-1")
                            .attributes(Map.of("sequence", "1"))
                            .build(),
                        Message.builder()
                            .data("second message")
                            .orderingKey("key-1")
                            .attributes(Map.of("sequence", "2"))
                            .build(),
                        Message.builder()
                            .orderingKey("key-2")
                            .attributes(Map.of("sequence", "3"))
                            .build(),
                        Message.builder()
                            .data("message without ordering key")
                            .attributes(Map.of("sequence", "4"))
                            .build()
                    )
                )
                .build();

            var publishOutput = publish.run(runContext);
            assertThat(publishOutput.getMessagesCount(), is(4));

            var consume = Consume.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .subscription(Property.ofValue(subscription))
                .maxRecords(Property.ofValue(4))
                .build();

            var consumeOutput = consume.run(runContextFactory.of());
            assertThat(consumeOutput.getCount(), is(4));
        }
    }

    @Test
    void shouldFailWhenTopicDoesNotExist() throws Exception {
        try (var connections = PubSubTestConnections.open(FLOCI_GCP.getEmulatorHost())) {
            String topic = "missing-" + IdUtils.create();
            var topicName = ProjectTopicName.of(project, topic);

            try (TopicAdminClient topicAdmin = connections.topicAdminClient()) {
                var missingTopic = assertThrows(NotFoundException.class, () -> topicAdmin.getTopic(topicName));
                assertThat(missingTopic.getStatusCode().getCode(), is(Code.NOT_FOUND));
            }

            var publish = Publish.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue(topic))
                .from(List.of(Message.builder().data("Hello World").build()))
                .build();

            var publishFailure = assertThrows(NotFoundException.class, () -> publish.run(runContextFactory.of()));
            assertThat(publishFailure.getStatusCode().getCode(), is(Code.NOT_FOUND));
            assertThat(publishFailure.getCause(), instanceOf(StatusRuntimeException.class));
            assertThat(((StatusRuntimeException) publishFailure.getCause()).getStatus().getCode(), is(Status.Code.NOT_FOUND));
        }
    }

    private URI createTestFile(RunContext runContext, PubSubTestUtils resources) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try {
            try (OutputStream output = new FileOutputStream(tempFile)) {
                FileSerde.write(
                    output,
                    Message.builder().data("Hello World").build()
                );
                FileSerde.write(
                    output,
                    Message.builder().attributes(Map.of("key", "value")).build()
                );
            }

            try (FileInputStream input = new FileInputStream(tempFile)) {
                URI uri = storageInterface.put(
                    TenantService.MAIN_TENANT,
                    null,
                    URI.create("/" + IdUtils.create() + ".ion"),
                    input
                );
                resources.registerCleanup(() -> storageInterface.delete(TenantService.MAIN_TENANT, null, uri));
                return uri;
            }
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    private String messageData(Object data) {
        if (data instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Objects.toString(data, null);
    }
}
