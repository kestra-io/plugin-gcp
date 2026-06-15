package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;

import io.grpc.ManagedChannelBuilder;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract class AbstractPubSub extends AbstractTask implements PubSubConnectionInterface {
    @NotNull
    @Schema(
        title = "Topic name",
        description = "Pub/Sub topic ID (without project prefix)"
    )
    @PluginProperty(group = "main")
    private Property<String> topic;

    /**
     * Returns an emulator channel provider when PUBSUB_EMULATOR_HOST is set, null otherwise.
     * The Java Pub/Sub SDK does not honor PUBSUB_EMULATOR_HOST automatically — we must
     * configure the transport channel and credentials explicitly for every client type.
     */
    static EmulatorConfig emulatorConfig() {
        var host = System.getenv("PUBSUB_EMULATOR_HOST");
        if (host == null || host.isBlank()) {
            return null;
        }
        var channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
        var channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        return new EmulatorConfig(channelProvider, NoCredentialsProvider.create());
    }

    record EmulatorConfig(
        FixedTransportChannelProvider channelProvider,
        NoCredentialsProvider credentialsProvider
    ) {}

    Publisher createPublisher(PublisherOptions options) throws IOException, IllegalVariableEvaluationException {
        RunContext runContext = options.getRunContext();
        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());

        Publisher.Builder builder = Publisher.newBuilder(topicName)
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()));

        var emulator = emulatorConfig();
        if (emulator != null) {
            builder.setChannelProvider(emulator.channelProvider())
                .setCredentialsProvider(emulator.credentialsProvider());
        } else {
            builder.setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)));
        }

        if (options.isEnableMessageOrdering()) {
            builder.setEnableMessageOrdering(true);
        }

        return builder.build();
    }

    public ProjectSubscriptionName createSubscription(RunContext runContext, String subscription, boolean autoCreateSubscription) throws IOException, IllegalVariableEvaluationException {

        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(subscription));

        if (autoCreateSubscription) {
            var subscriptionAdminSettingsBuilder = SubscriptionAdminSettings.newBuilder()
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()));

            var emulator = emulatorConfig();
            if (emulator != null) {
                subscriptionAdminSettingsBuilder
                    .setTransportChannelProvider(emulator.channelProvider())
                    .setCredentialsProvider(emulator.credentialsProvider());
            } else {
                subscriptionAdminSettingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)));
            }

            // List all existing subscriptions and create the subscription if needed
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettingsBuilder.build())) {
                Iterable<Subscription> subscriptions = subscriptionAdminClient.listSubscriptions(ProjectName.of(runContext.render(projectId).as(String.class).orElse(null)))
                    .iterateAll();
                Optional<Subscription> existing = StreamSupport.stream(subscriptions.spliterator(), false)
                    .filter(sub -> sub.getName().equals(subscriptionName.toString()))
                    .findFirst();
                if (existing.isEmpty()) {
                    subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
                }
            }
        }

        return subscriptionName;
    }

    @Getter
    @Builder
    public static class PublisherOptions {
        @NonNull
        private final RunContext runContext;

        @Builder.Default
        private final boolean enableMessageOrdering = false;
    }
}
