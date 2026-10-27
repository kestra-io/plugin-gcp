package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;

import io.kestra.core.utils.IdUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Creates and cleans up real Pub/Sub resources for integration tests.
 */
final class PubSubTestUtils implements AutoCloseable {
    private final String project;
    private final TopicAdminClient topicAdmin;
    private final SubscriptionAdminClient subscriptionAdmin;
    private final List<String> topics = new ArrayList<>();
    private final List<String> subscriptions = new ArrayList<>();
    private final List<CleanupRegistration> activeResources = new ArrayList<>();
    private final List<Cleanup> additionalCleanup = new ArrayList<>();
    private boolean closed;

    private PubSubTestUtils(String project, TopicAdminClient topicAdmin, SubscriptionAdminClient subscriptionAdmin) {
        this.project = project;
        this.topicAdmin = topicAdmin;
        this.subscriptionAdmin = subscriptionAdmin;
    }

    static PubSubTestUtils forFloci(String project, PubSubTestConnections connections) throws IOException {
        TopicAdminClient topicAdmin = connections.topicAdminClient();
        try {
            return new PubSubTestUtils(project, topicAdmin, connections.subscriptionAdminClient());
        } catch (IOException | RuntimeException | Error failure) {
            closeAfterFailure(failure, topicAdmin);
            throw failure;
        }
    }

    static PubSubTestUtils withDefaultCredentials(String project) throws IOException {
        TopicAdminClient topicAdmin = TopicAdminClient.create();
        try {
            return new PubSubTestUtils(project, topicAdmin, SubscriptionAdminClient.create());
        } catch (IOException | RuntimeException | Error failure) {
            closeAfterFailure(failure, topicAdmin);
            throw failure;
        }
    }

    String createTopic() throws IOException {
        ensureOpen();
        String topic = "test-topic-" + IdUtils.create();
        topicAdmin.createTopic(ProjectTopicName.of(project, topic));
        topics.add(topic);
        return topic;
    }

    String createSubscription(String topic) throws IOException {
        ensureOpen();
        String subscription = "test-subscription-" + IdUtils.create();
        subscriptionAdmin.createSubscription(
            ProjectSubscriptionName.of(project, subscription),
            ProjectTopicName.of(project, topic),
            PushConfig.getDefaultInstance(),
            10
        );
        subscriptions.add(subscription);
        return subscription;
    }

    void assertSubscriptionAcknowledged(String subscription) throws IOException {
        ensureOpen();
        var pendingMessages = subscriptionAdmin.pull(
            PullRequest.newBuilder()
                .setSubscription(ProjectSubscriptionName.of(project, subscription).toString())
                .setMaxMessages(10)
                .setReturnImmediately(true)
                .build()
        );
        assertTrue(
            pendingMessages.getReceivedMessagesList().isEmpty(),
            "Expected the subscription backlog to be empty after acknowledgement"
        );
    }

    CleanupRegistration registerActiveResource(Cleanup cleanup) {
        ensureOpen();
        var registration = new CleanupRegistration(cleanup);
        activeResources.add(registration);
        return registration;
    }

    void registerCleanup(Cleanup cleanup) {
        ensureOpen();
        additionalCleanup.add(cleanup);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        Throwable failure = null;
        for (int i = activeResources.size() - 1; i >= 0; i--) {
            failure = attempt(failure, activeResources.get(i)::close);
        }
        for (int i = subscriptions.size() - 1; i >= 0; i--) {
            String subscription = subscriptions.get(i);
            failure = attempt(
                failure,
                () -> subscriptionAdmin.deleteSubscription(ProjectSubscriptionName.of(project, subscription))
            );
        }
        for (int i = topics.size() - 1; i >= 0; i--) {
            String topic = topics.get(i);
            failure = attempt(failure, () -> topicAdmin.deleteTopic(ProjectTopicName.of(project, topic)));
        }
        for (int i = additionalCleanup.size() - 1; i >= 0; i--) {
            failure = attempt(failure, additionalCleanup.get(i));
        }

        failure = attempt(failure, subscriptionAdmin::close);
        failure = attempt(failure, topicAdmin::close);
        throwIfFailed(failure);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Pub/Sub test resources have already been closed");
        }
    }

    private static Throwable attempt(Throwable failure, Cleanup cleanup) {
        try {
            cleanup.run();
        } catch (Throwable cleanupFailure) {
            if (failure == null) {
                return cleanupFailure;
            }
            if (failure != cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        return failure;
    }

    private static void throwIfFailed(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("Unexpected Pub/Sub cleanup failure", failure);
        }
    }

    private static void closeAfterFailure(Throwable failure, AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @FunctionalInterface
    interface Cleanup {
        void run() throws Exception;
    }

    static final class CleanupRegistration implements AutoCloseable {
        private final Cleanup cleanup;
        private boolean completed;

        private CleanupRegistration(Cleanup cleanup) {
            this.cleanup = cleanup;
        }

        @Override
        public synchronized void close() throws Exception {
            if (completed) {
                return;
            }
            cleanup.run();
            completed = true;
        }
    }
}
