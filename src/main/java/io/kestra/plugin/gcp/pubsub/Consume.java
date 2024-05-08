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
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.validation.constraints.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from a Pub/Sub topic.",
    description = "Requires a maxDuration or a maxRecords."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: topic-test",
                "maxRecords: 10"
            }
        )
    }
)
public class Consume extends AbstractPubSub implements RunnableTask<Consume.Output> {

    @Schema(
        title = "The Pub/Sub subscription.",
        description = "The Pub/Sub subscription. It will be created automatically if it didn't exist and 'autoCreateSubscription' is enabled."
    )
    @PluginProperty(dynamic = true)
    private String subscription;

    @Schema(
        title = "Whether the Pub/Sub subscription should be created if not exists."
    )
    @PluginProperty
    @Builder.Default
    private Boolean autoCreateSubscription = true;

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("'maxDuration' or 'maxRecords' must be set to avoid an infinite loop");
        }

        var subscriptionName = this.createSubscription(runContext, subscription, autoCreateSubscription);
        var total = new AtomicInteger();
        var started = ZonedDateTime.now();
        var tempFile = runContext.tempFile(".ion").toFile();

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            AtomicReference<Exception> threadException = new AtomicReference<>();
            MessageReceiver receiver = (message, consumer) -> {
                try {
                    FileSerde.write(outputFile, Message.of(message, serdeType));
                    total.getAndIncrement();
                    consumer.ack();
                }
                catch(Exception e) {
                    threadException.set(e);
                    consumer.nack();
                }
            };
            var subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
                .build();
            subscriber.startAsync().awaitRunning();

            while (!this.ended(total, started)) {
                if (threadException.get() != null) {
                    subscriber.stopAsync().awaitTerminated();
                    throw threadException.get();
                }
                Thread.sleep(100);
            }
            subscriber.stopAsync().awaitTerminated();

            runContext.metric(Counter.of("records", total.get(), "topic", runContext.render(this.getTopic())));
            outputFile.flush();
        }
        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .count(total.get())
            .build();
    }

    public Publisher<Message> stream(RunContext runContext) throws Exception {
	    ProjectSubscriptionName subscriptionName = this.createSubscription(runContext, subscription, autoCreateSubscription);
        GoogleCredentials credentials = this.credentials(runContext);

        return Flux.<Message>create(
                sink -> {
	                AtomicInteger total = new AtomicInteger();
	                ZonedDateTime started = ZonedDateTime.now();

                    MessageReceiver receiver = (message, consumer) -> {
                        try {
                            sink.next(Message.of(message, serdeType));
                            total.getAndIncrement();
                            consumer.ack();
                        }
                        catch(Exception exception) {
                            sink.error(exception);
                            consumer.nack();
                        }
                    };

                    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .build();

                    subscriber.startAsync().awaitRunning();

                    subscriber.addListener(
                        new ApiService.Listener() {
                            @Override
                            public void failed(ApiService.State from, Throwable failure) {
                                sink.error(failure);
                            }
                        }, MoreExecutors.directExecutor()
                    );

                    while (!this.ended(total, started)) {
                        if (sink.isCancelled()) {
                            subscriber.stopAsync().awaitTerminated();
                            return;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            sink.error(exception);
                        }
                    }

                    subscriber.stopAsync().awaitTerminated();
                    sink.complete();
                },
                FluxSink.OverflowStrategy.BUFFER
            )
            .subscribeOn(Schedulers.boundedElastic());
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start) {
        if (this.maxRecords != null && count.get() >= this.maxRecords) {
            return true;
        }
        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of consumed rows."
        )
        private final Integer count;
        @Schema(
            title = "File URI containing consumed messages."
        )
        private final URI uri;
    }
}
