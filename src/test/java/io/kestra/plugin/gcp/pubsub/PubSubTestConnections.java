package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

import org.mockito.MockedStatic;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.TopicName;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Supplies real Pub/Sub clients configured for Floci to integration tests without adding emulator
 * connection state to production code.
 *
 * <p>
 * The Google client builders are invoked synchronously by these tasks. In particular,
 * {@link RealtimeTrigger} creates its subscriber when the test subscribes directly to the returned
 * Flux. This scope must remain open around task execution and that subscription.
 * </p>
 */
final class PubSubTestConnections implements AutoCloseable {
    private final String endpoint;
    private final Thread owner;
    private final MockedStatic<GoogleCredentials> credentials;
    private final MockedStatic<FixedCredentialsProvider> fixedCredentialsProviders;
    private final MockedStatic<Publisher> publishers;
    private final MockedStatic<Subscriber> subscribers;
    private final MockedStatic<SubscriptionAdminSettings> subscriptionSettings;

    private PubSubTestConnections(
        String endpoint,
        Thread owner,
        MockedStatic<GoogleCredentials> credentials,
        MockedStatic<FixedCredentialsProvider> fixedCredentialsProviders,
        MockedStatic<Publisher> publishers,
        MockedStatic<Subscriber> subscribers,
        MockedStatic<SubscriptionAdminSettings> subscriptionSettings) {
        this.endpoint = endpoint;
        this.owner = owner;
        this.credentials = credentials;
        this.fixedCredentialsProviders = fixedCredentialsProviders;
        this.publishers = publishers;
        this.subscribers = subscribers;
        this.subscriptionSettings = subscriptionSettings;
    }

    static PubSubTestConnections open(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Pub/Sub emulator endpoint must not be blank");
        }

        Thread owner = Thread.currentThread();
        GoogleCredentials emulatorCredentials = GoogleCredentials.create(
            new AccessToken("floci-test-token", Date.from(Instant.now().plusSeconds(3600)))
        );
        MockedStatic<GoogleCredentials> credentials = mockStatic(GoogleCredentials.class, CALLS_REAL_METHODS);
        MockedStatic<FixedCredentialsProvider> fixedCredentialsProviders = null;
        MockedStatic<Publisher> publishers = null;
        MockedStatic<Subscriber> subscribers = null;
        MockedStatic<SubscriptionAdminSettings> subscriptionSettings = null;

        try {
            credentials.when(GoogleCredentials::getApplicationDefault).thenReturn(emulatorCredentials);

            // Production task builders always install a FixedCredentialsProvider. Return one with no
            // credentials so the real task builders can call that path safely over Floci plaintext.
            FixedCredentialsProvider noCredentials = new FixedCredentialsProvider() {
                @Override
                public Credentials getCredentials() {
                    return null;
                }
            };
            fixedCredentialsProviders = mockStatic(FixedCredentialsProvider.class, CALLS_REAL_METHODS);
            fixedCredentialsProviders.when(() -> FixedCredentialsProvider.create(any(Credentials.class))).thenReturn(noCredentials);

            publishers = mockStatic(Publisher.class, CALLS_REAL_METHODS);
            publishers.when(() -> Publisher.newBuilder(any(TopicName.class))).thenAnswer(invocation ->
            {
                assertOwnerThread(owner);
                Publisher.Builder builder = (Publisher.Builder) invocation.callRealMethod();
                return builder
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .setChannelProvider(channelProvider(endpoint));
            });

            subscribers = mockStatic(Subscriber.class, CALLS_REAL_METHODS);
            subscribers.when(() -> Subscriber.newBuilder(any(ProjectSubscriptionName.class), any(MessageReceiver.class)))
                .thenAnswer(invocation ->
                {
                    assertOwnerThread(owner);
                    Subscriber.Builder builder = (Subscriber.Builder) invocation.callRealMethod();
                    return builder
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setChannelProvider(channelProvider(endpoint));
                });

            subscriptionSettings = mockStatic(SubscriptionAdminSettings.class, CALLS_REAL_METHODS);
            subscriptionSettings.when(SubscriptionAdminSettings::newBuilder).thenAnswer(invocation ->
            {
                assertOwnerThread(owner);
                SubscriptionAdminSettings.Builder builder = (SubscriptionAdminSettings.Builder) invocation.callRealMethod();
                return builder.setTransportChannelProvider(channelProvider(endpoint));
            });

            return new PubSubTestConnections(endpoint, owner, credentials, fixedCredentialsProviders, publishers, subscribers, subscriptionSettings);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(failure, subscriptionSettings, subscribers, publishers, fixedCredentialsProviders, credentials);
            throw failure;
        }
    }

    TopicAdminClient topicAdminClient() throws IOException {
        TopicAdminSettings settings = TopicAdminSettings.newBuilder()
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(channelProvider(endpoint))
            .build();
        return TopicAdminClient.create(settings);
    }

    SubscriptionAdminClient subscriptionAdminClient() throws IOException {
        SubscriptionAdminSettings settings = SubscriptionAdminSettings.newBuilder()
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setTransportChannelProvider(channelProvider(endpoint))
            .build();
        return SubscriptionAdminClient.create(settings);
    }

    @Override
    public void close() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Pub/Sub test connection scope must close on the thread that opened it");
        }

        try {
            subscriptionSettings.close();
        } finally {
            try {
                subscribers.close();
            } finally {
                try {
                    publishers.close();
                } finally {
                    try {
                        fixedCredentialsProviders.close();
                    } finally {
                        credentials.close();
                    }
                }
            }
        }
    }

    private static InstantiatingGrpcChannelProvider channelProvider(String endpoint) {
        return InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint(endpoint)
            .setChannelConfigurator(channelBuilder -> channelBuilder.usePlaintext())
            .build();
    }

    private static void assertOwnerThread(Thread owner) {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Pub/Sub test connection must be created on the test thread");
        }
    }

    private static void closeAfterFailure(Throwable failure, AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
