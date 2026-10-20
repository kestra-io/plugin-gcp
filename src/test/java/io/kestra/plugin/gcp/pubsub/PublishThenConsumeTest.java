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
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;

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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
class PublishThenConsumeTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void runWithList() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            var runContext = runContextFactory.of();
            String topic = null;
            String subscription = null;
            URI consumedMessagesUri = null;

            try {
                topic = createTopic();
                subscription = createSubscription(topic);

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
                consumedMessagesUri = consumeOutput.getUri();
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

                try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
                    var pendingMessages = subscriptionAdmin.pull(
                        PullRequest.newBuilder()
                            .setSubscription(ProjectSubscriptionName.of(project, subscription).toString())
                            .setMaxMessages(10)
                            .setReturnImmediately(true)
                            .build()
                    );
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

    @Test
    void runWithJson() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            String topic = null;
            String subscription = null;
            URI consumedMessagesUri = null;

            try {
                var runContext = runContextFactory.of();
                topic = createTopic();
                subscription = createSubscription(topic);

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
                consumedMessagesUri = consumeOutput.getUri();
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

                assertSubscriptionAcknowledged(subscription);
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

    @Test
    void runWithFile() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            String topic = null;
            String subscription = null;
            URI sourceUri = null;
            URI consumedMessagesUri = null;

            try {
                var runContext = runContextFactory.of();
                topic = createTopic();
                subscription = createSubscription(topic);
                sourceUri = createTestFile(runContext);

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
                consumedMessagesUri = consumeOutput.getUri();
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

                assertSubscriptionAcknowledged(subscription);
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
                        try {
                            if (sourceUri != null) {
                                storageInterface.delete(TenantService.MAIN_TENANT, null, sourceUri);
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
    }

    // Floci 0.11.0 does not faithfully emulate ordered-subscription delivery semantics.
    @Test
    @EnabledIfEnvironmentVariable(
        named = "GOOGLE_APPLICATION_CREDENTIALS",
        matches = ".+",
        disabledReason = "Floci 0.11.0 does not faithfully emulate ordered-subscription delivery semantics."
    )
    void runWithMessagesWithOrderingKey() throws Exception {
        var runContext = runContextFactory.of();
        String topic = createTopic();
        String subscription = createSubscription(topic);

        try {
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
        } finally {
            deleteTopic(topic);
        }
    }

    @Test
    void shouldFailWhenTopicDoesNotExist() throws Exception {
        try (var emulatorScope = PubSubClientConnection.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            String topic = "missing-" + IdUtils.create();
            var topicName = ProjectTopicName.of(project, topic);

            try (TopicAdminClient topicAdmin = PubSubClientConnection.current().createTopicAdminClient()) {
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

    private URI createTestFile(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try {
            try (OutputStream output = new FileOutputStream(tempFile)) {
                FileSerde.write(
                    output,
                    Message.builder().data("Hello World".getBytes(StandardCharsets.UTF_8)).build()
                );
                FileSerde.write(
                    output,
                    Message.builder().attributes(Map.of("key", "value")).build()
                );
            }

            try (FileInputStream input = new FileInputStream(tempFile)) {
                return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
            }
        } finally {
            Files.deleteIfExists(tempFile.toPath());
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

    private String createTopic() throws Exception {
        String topicId = "test-topic-" + IdUtils.create();
        try (TopicAdminClient client = PubSubClientConnection.current().createTopicAdminClient()) {
            client.createTopic(ProjectTopicName.of(project, topicId));
        }
        return topicId;
    }

    private void deleteSubscription(String subscription) throws Exception {
        try (SubscriptionAdminClient client = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            client.deleteSubscription(ProjectSubscriptionName.of(project, subscription));
        }
    }

    private void deleteTopic(String topic) throws Exception {
        try (TopicAdminClient client = PubSubClientConnection.current().createTopicAdminClient()) {
            client.deleteTopic(ProjectTopicName.of(project, topic));
        }
    }

    private void assertSubscriptionAcknowledged(String subscription) throws Exception {
        try (var subscriptionAdmin = PubSubClientConnection.current().createSubscriptionAdminClient()) {
            var pendingMessages = subscriptionAdmin.pull(
                PullRequest.newBuilder()
                    .setSubscription(ProjectSubscriptionName.of(project, subscription).toString())
                    .setMaxMessages(10)
                    .setReturnImmediately(true)
                    .build()
            );
            assertThat(pendingMessages.getReceivedMessagesList(), is(empty()));
        }
    }

    private String messageData(Object data) {
        if (data instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return Objects.toString(data, null);
    }
}
