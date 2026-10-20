package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.util.Objects;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

final class PubSubClientConnection {
    private static final ThreadLocal<String> EMULATOR_ENDPOINT = new ThreadLocal<>();

    private final String emulatorEndpoint;

    private PubSubClientConnection(String emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    static PubSubClientConnection current() {
        return new PubSubClientConnection(EMULATOR_ENDPOINT.get());
    }

    static EmulatorScope useEmulatorEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Pub/Sub emulator endpoint must not be blank");
        }

        String previousEndpoint = EMULATOR_ENDPOINT.get();
        EMULATOR_ENDPOINT.set(endpoint);
        return new EmulatorScope(previousEndpoint, Thread.currentThread());
    }

    boolean usesEmulator() {
        return emulatorEndpoint != null;
    }

    void configure(Publisher.Builder builder, GoogleCredentials credentials) {
        builder.setCredentialsProvider(credentialsProvider(credentials));
        if (usesEmulator()) {
            builder.setChannelProvider(channelProvider());
        }
    }

    void configure(Subscriber.Builder builder, GoogleCredentials credentials) {
        builder.setCredentialsProvider(credentialsProvider(credentials));
        if (usesEmulator()) {
            builder.setChannelProvider(channelProvider());
        }
    }

    void configure(SubscriptionAdminSettings.Builder builder, GoogleCredentials credentials) {
        builder.setCredentialsProvider(credentialsProvider(credentials));
        if (usesEmulator()) {
            builder.setTransportChannelProvider(channelProvider());
        }
    }

    TopicAdminClient createTopicAdminClient() throws IOException {
        if (!usesEmulator()) {
            return TopicAdminClient.create();
        }

        TopicAdminSettings.Builder settings = TopicAdminSettings.newBuilder();
        settings.setCredentialsProvider(NoCredentialsProvider.create());
        settings.setTransportChannelProvider(channelProvider());
        return TopicAdminClient.create(settings.build());
    }

    SubscriptionAdminClient createSubscriptionAdminClient() throws IOException {
        if (!usesEmulator()) {
            return SubscriptionAdminClient.create();
        }

        SubscriptionAdminSettings.Builder settings = SubscriptionAdminSettings.newBuilder();
        settings.setCredentialsProvider(NoCredentialsProvider.create());
        settings.setTransportChannelProvider(channelProvider());
        return SubscriptionAdminClient.create(settings.build());
    }

    private CredentialsProvider credentialsProvider(GoogleCredentials credentials) {
        if (usesEmulator()) {
            return NoCredentialsProvider.create();
        }
        return FixedCredentialsProvider.create(Objects.requireNonNull(credentials, "Google credentials are required outside emulator mode"));
    }

    private InstantiatingGrpcChannelProvider channelProvider() {
        return InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint(emulatorEndpoint)
            .setChannelConfigurator(channelBuilder -> channelBuilder.usePlaintext())
            .build();
    }

    static final class EmulatorScope implements AutoCloseable {
        private final String previousEndpoint;
        private final Thread owner;
        private boolean closed;

        private EmulatorScope(String previousEndpoint, Thread owner) {
            this.previousEndpoint = previousEndpoint;
            this.owner = owner;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("Pub/Sub emulator scope must be closed by the thread that opened it");
            }

            if (previousEndpoint == null) {
                EMULATOR_ENDPOINT.remove();
            } else {
                EMULATOR_ENDPOINT.set(previousEndpoint);
            }
            closed = true;
        }
    }
}
