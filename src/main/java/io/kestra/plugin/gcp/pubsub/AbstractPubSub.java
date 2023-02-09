package io.kestra.plugin.gcp.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.VersionProvider;
import io.kestra.plugin.gcp.AbstractTask;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
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
    private String topic;

    Publisher createPublisher(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);

        TopicName topicName = TopicName.of(runContext.render(projectId), runContext.render(topic));
        return Publisher.newBuilder(topicName)
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + versionProvider.getVersion()))
            .build();
    }

    ProjectSubscriptionName createSubscription(RunContext runContext, String subscription, boolean autoCreateSubscription)
        throws IOException, IllegalVariableEvaluationException {
        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);

        TopicName topicName = TopicName.of(runContext.render(projectId), runContext.render(topic));
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName
            .of(runContext.render(projectId), runContext.render(subscription));

        if (autoCreateSubscription) {
            SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
                .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + versionProvider.getVersion()))
                .build();

            // List all existing subscriptions and create the subscription if needed
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)) {
                Iterable<Subscription> subscriptions = subscriptionAdminClient
                    .listSubscriptions(ProjectName.of(runContext.render(projectId)))
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
}
