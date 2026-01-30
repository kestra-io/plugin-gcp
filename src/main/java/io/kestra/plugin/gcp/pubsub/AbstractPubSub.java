package io.kestra.plugin.gcp.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

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
    private Property<String> topic;

    Publisher createPublisher(PublisherOptions options) throws IOException, IllegalVariableEvaluationException {
        RunContext runContext = options.getRunContext();
        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());

        Publisher.Builder builder = Publisher.newBuilder(topicName)
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()));

        if (options.isEnableMessageOrdering()) {
            builder.setEnableMessageOrdering(true);
        }

        return builder.build();
    }

    public ProjectSubscriptionName createSubscription(RunContext runContext, String subscription, boolean autoCreateSubscription) throws IOException, IllegalVariableEvaluationException {

        TopicName topicName = TopicName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(topic).as(String.class).orElseThrow());
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(runContext.render(projectId).as(String.class).orElse(null), runContext.render(subscription));

        if(autoCreateSubscription) {
            SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
                .build();

            // List all existing subscriptions and create the subscription if needed
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)) {
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
