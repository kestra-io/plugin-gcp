package io.kestra.plugin.gcp.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.validation.constraints.NotNull;

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
            full = true,
            code = """
                id: gcp_pubsub_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.gcp.pubsub.Consume
                    topic: topic-test
                    maxRecords: 10
                    projectId: {{ secret('GCP_PROJECT_ID') }}
                    subscription: my-subscription
                """
        )
    }
)
public class Consume extends AbstractPubSub implements RunnableTask<Consume.Output> {

    @Schema(
        title = "The Pub/Sub subscription.",
        description = "The Pub/Sub subscription. It will be created automatically if it didn't exist and 'autoCreateSubscription' is enabled."
    )
    @NotNull
    private Property<String> subscription;

    @Schema(
        title = "Whether the Pub/Sub subscription should be created if not exists."
    )
    @Builder.Default
    private Property<Boolean> autoCreateSubscription = Property.of(true);

    @Schema(title = "Max number of records, when reached the task will end.")
    private Property<Integer> maxRecords;

    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Property<Duration> maxDuration;

    @Builder.Default
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("'maxDuration' or 'maxRecords' must be set to avoid an infinite loop");
        }

        var subscriptionName = this.createSubscription(
            runContext,
            runContext.render(subscription).as(String.class).orElseThrow(),
            runContext.render(autoCreateSubscription).as(Boolean.class).orElseThrow()
        );
        var total = new AtomicInteger();
        var started = ZonedDateTime.now();
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            AtomicReference<Exception> threadException = new AtomicReference<>();
            MessageReceiver receiver = (message, consumer) -> {
                try {
                    FileSerde.write(outputFile, Message.of(message, runContext.render(serdeType).as(SerdeType.class).orElseThrow()));
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

            while (!this.ended(total, started, runContext)) {
                if (threadException.get() != null) {
                    subscriber.stopAsync().awaitTerminated();
                    throw threadException.get();
                }
                Thread.sleep(100);
            }
            subscriber.stopAsync().awaitTerminated();

            runContext.metric(Counter.of("records", total.get(), "topic", runContext.render(this.getTopic()).as(String.class).orElseThrow()));
            outputFile.flush();
        }
        return Output.builder()
            .uri(runContext.storage().putFile(tempFile))
            .count(total.get())
            .build();
    }

    private boolean ended(AtomicInteger count, ZonedDateTime start, RunContext runContext) throws IllegalVariableEvaluationException {
        var max = runContext.render(this.maxRecords).as(Integer.class);
        if (max.isPresent() && count.get() >= max.get()) {
            return true;
        }

        var duration = runContext.render(this.maxDuration).as(Duration.class);
        return duration.isPresent() && ZonedDateTime.now().toEpochSecond() > start.plus(duration.get()).toEpochSecond();
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
