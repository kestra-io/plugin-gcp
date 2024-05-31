package io.kestra.plugin.gcp.pubsub;

import com.google.api.core.ApiService;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume a message in real-time from a Pub/Sub topic and create one execution per message.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.gcp.pubsub.Trigger](https://kestra.io/plugins/plugin-gcp/triggers/io.kestra.plugin.gcp.pubsub.trigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a Pub/Sub topic in real-time.",
            code = """
                id: realtime-pubsub
                namespace: dev

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "Received: {{ trigger.data }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.gcp.pubsub.RealtimeTrigger
                    projectId: test-project-id
                    topic: test-topic
                    subscription: test-subscription
                """,
            full = true
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.Output>, PubSubConnectionInterface {

    private String projectId;

    private String serviceAccount;

    @Builder.Default
    private List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    private String topic;

    @Schema(
        title = "The Pub/Sub subscription",
        description = "The Pub/Sub subscription. It will be created automatically if it didn't exist and 'autoCreateSubscription' is enabled."
    )
    @PluginProperty(dynamic = true)
    private String subscription;

    @Schema(
        title = "Whether the Pub/Sub subscription should be created if not exist"
    )
    @PluginProperty
    @Builder.Default
    private Boolean autoCreateSubscription = true;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<Subscriber> subscriberReference = new AtomicReference<>();

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        Consume task = Consume.builder()
            .topic(this.topic)
            .subscription(this.subscription)
            .autoCreateSubscription(this.autoCreateSubscription)
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .scopes(this.scopes)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .serdeType(this.serdeType)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(message -> TriggerService.generateRealtimeExecution(this, context, message));
    }

    private Publisher<Message> publisher(final Consume task, final RunContext runContext) throws Exception {
        ProjectSubscriptionName subscriptionName = task.createSubscription(runContext, subscription, autoCreateSubscription);
        GoogleCredentials credentials = task.credentials(runContext);

        return Flux.create(
            emitter -> {
                AtomicInteger total = new AtomicInteger();
                final MessageReceiver receiver = (message, consumer) -> {
                    try {
                        emitter.next(Message.of(message, serdeType));
                        total.getAndIncrement();
                        consumer.ack();
                    }  catch(Exception exception) {
                        emitter.error(exception);
                        consumer.nack();
                    }
                };

                Subscriber subscriber = Subscriber
                    .newBuilder(subscriptionName, receiver)
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();

                this.subscriberReference.set(subscriber);

                try {
                    subscriber.startAsync().awaitRunning();
                    subscriber.addListener(
                        new ApiService.Listener() {
                            @Override
                            public void failed(ApiService.State from, Throwable failure) {
                                emitter.error(failure);
                                waitForTermination.countDown();
                            }

                            @Override
                            public void terminated(ApiService.State from) {
                                emitter.complete();
                                waitForTermination.countDown();
                            }
                        }, MoreExecutors.directExecutor()
                    );
                } catch (Exception exception) {
                    if (subscriber.isRunning()) {
                        subscriber.stopAsync().awaitTerminated();
                    }
                    emitter.error(exception);
                    waitForTermination.countDown();
                }
            });
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        Optional.ofNullable(subscriberReference.get()).ifPresent(subscriber -> {
            subscriber.stopAsync(); // Shut down the PubSub subscriber.
            if (wait) {
                try {
                    this.waitForTermination.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}
