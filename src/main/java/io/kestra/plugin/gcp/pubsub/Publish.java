package io.kestra.plugin.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish a message to a Pub/Sub topic"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: topic-test",
                "from:",
                "-  data: \"{{ 'base64-encoded-string-1' | base64encode }}\"",
                "   attributes:",
                "       testAttribute: KestraTest",
                "   messageId: '1234'",
                "   orderingKey: 'foo'",
                "-  data: \"{{ 'base64-encoded-string-2' | base64encode }}\"",
                "-  attributes:",
                "       testAttribute: KestraTest"
            }
        )
    }
)
public class Publish extends AbstractPubSub implements RunnableTask<Publish.Output> {
    @PluginProperty(dynamic = true)
    @NotNull
    @Schema(
        title = "The source of the published data.",
        description = "Can be an internal storage URI, a list of Pub/Sub messages, or a single Pub/Sub message.",
        anyOf = {String.class, Message[].class, Message.class}
    )
    private Object from;

    @Builder.Default
    @PluginProperty
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private SerdeType serdeType = SerdeType.STRING;

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        var publisher = this.createPublisher(runContext);
        Integer count = 1;
        Flowable<Message> flowable;
        Flowable<Integer> resultFlowable;

        if (this.from instanceof String) {
            URI from = new URI(runContext.render((String) this.from));
            if(!from.getScheme().equals("kestra")) {
                throw new Exception("Invalid from parameter, must be a Kestra internal storage URI");
            }

            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                flowable = Flowable.create(FileSerde.reader(inputStream, Message.class), BackpressureStrategy.BUFFER);
                resultFlowable = this.buildFlowable(flowable, publisher, runContext);

                count = resultFlowable.reduce(Integer::sum).blockingGet();
            }

        } else if (this.from instanceof List) {
            flowable = Flowable
                .fromArray(((List<Object>) this.from).toArray())
                .map(o -> JacksonMapper.toMap(o, Message.class));

            resultFlowable = this.buildFlowable(flowable, publisher, runContext);

            count = resultFlowable.reduce(Integer::sum).blockingGet();
        } else {
            var msg = JacksonMapper.toMap(this.from, Message.class);
            publisher.publish(msg.to(runContext, this.serdeType));
        }

        publisher.shutdown();

        // metrics
        runContext.metric(Counter.of("records", count, "topic", runContext.render(this.getTopic())));

        return Output.builder()
            .messagesCount(count)
        .build();
    }

    private Flowable<Integer> buildFlowable(Flowable<Message> flowable, Publisher publisher, RunContext runContext) {
        return flowable
            .map(message -> {
                publisher.publish(message.to(runContext, this.serdeType));
                return 1;
            });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of published messages.")
        private final Integer messagesCount;
    }
}
