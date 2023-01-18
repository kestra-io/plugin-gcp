package io.kestra.plugin.gcp.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.gcp.pubsub.model.Message;
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

import static io.kestra.core.utils.Rethrow.throwRunnable;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from an AMQP queue.",
    description = "Required a maxDuration or a maxRecords."
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
        title = "The PubSub subscription",
        description = "The PubSub subscription. It will be created automatically if it didn't exist."
    )
    @PluginProperty(dynamic = true)
    private String subscription;

    @PluginProperty
    @Schema(title = "Max number of records, when reached the task will end.")
    private Integer maxRecords;

    @PluginProperty
    @Schema(title = "Max duration in the Duration ISO format, after that the task will end.")
    private Duration maxDuration;


    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.maxDuration == null && this.maxRecords == null) {
            throw new IllegalArgumentException("maxDuration or maxRecords must be set to avoid infinite loop");
        }

        var subscriptionName = this.createSubscription(runContext, getSubscription());
        var total = new AtomicInteger();
        var started = ZonedDateTime.now();
        var tempFile = runContext.tempFile(".ion").toFile();

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            AtomicReference<Exception> threadException = new AtomicReference<>();
            MessageReceiver receiver = (message, consumer) -> {
                try {
                    FileSerde.write(outputFile, Message.of(message));
                    total.getAndIncrement();
                    consumer.ack();
                }
                catch(Exception e) {
                    threadException.set(e);
                }
            };
            var subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();

            Thread thread = new Thread(throwRunnable(() -> {
                subscriber.startAsync().awaitRunning();
            }));
            thread.setDaemon(true);
            thread.setName("pubsub-consume");
            thread.start();

            while (!this.ended(total, started)) {
                if (threadException.get() != null) {
                    subscriber.stopAsync().awaitTerminated();
                    thread.join();
                    throw threadException.get();
                }
                Thread.sleep(100);
            }
            subscriber.stopAsync().awaitTerminated();
            thread.join();

            runContext.metric(Counter.of("records", total.get(), "topic", runContext.render(this.getTopic())));
            outputFile.flush();
        }
        return Output.builder()
            .uri(runContext.putTempFile(tempFile))
            .count(total.get())
            .build();
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
