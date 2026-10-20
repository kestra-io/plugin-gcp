package io.kestra.plugin.gcp.pubsub;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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

    Publisher createPublisher(PublisherOptions options) throws IOException, IllegalVariableEvaluationException {
        RunContext runContext = options.getRunContext();
        var connection = PubSubClientConnection.current();
        // Outside emulator mode, credential resolution also backfills the inferred projectId.
        var credentials = connection.usesEmulator() ? null : this.credentials(runContext);
        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());

        Publisher.Builder builder = Publisher.newBuilder(topicName)
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()));
        connection.configure(builder, credentials);

        if (options.isEnableMessageOrdering()) {
            builder.setEnableMessageOrdering(true);
        }

        return builder.build();
    }

    public ProjectSubscriptionName createSubscription(RunContext runContext, String subscription, boolean autoCreateSubscription) throws IOException, IllegalVariableEvaluationException {
        var connection = PubSubClientConnection.current();
        // Outside emulator mode, credential resolution also backfills the inferred projectId.
        var credentials = connection.usesEmulator() ? null : this.credentials(runContext);
        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(subscription));

        if (autoCreateSubscription) {
            SubscriptionAdminSettings.Builder subscriptionAdminSettingsBuilder = SubscriptionAdminSettings.newBuilder()
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()));
            connection.configure(subscriptionAdminSettingsBuilder, credentials);

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
