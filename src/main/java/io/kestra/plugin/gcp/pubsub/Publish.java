package io.kestra.plugin.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
            full = true,
            code = """
                id: gcp_pubsub_publish
                namespace: company.team

                tasks:
                  - id: publish
                    type: io.kestra.plugin.gcp.pubsub.Publish
                    topic: topic-test
                    from:
                      - data: "{{ 'base64-encoded-string-1' | base64encode }}"
                         attributes:
                             testAttribute: KestraTest
                      - messageId: '1234'
                      - orderingKey: 'foo'
                      - data: "{{ 'base64-encoded-string-2' | base64encode }}"
                      - attributes:
                             testAttribute: KestraTest
                """
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
    @NotNull
    @Schema(title = "The serializer/deserializer to use.")
    private Property<SerdeType> serdeType = Property.of(SerdeType.STRING);

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        var publisher = this.createPublisher(runContext);
        Integer count = 1;
        Flux<Message> flowable;
        Flux<Integer> resultFlowable;

        if (this.from instanceof String) {
            URI from = new URI(runContext.render((String) this.from));
            if(!from.getScheme().equals("kestra")) {
                throw new Exception("Invalid from parameter, must be a Kestra internal storage URI");
            }

            try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(from)))) {
                flowable = FileSerde.readAll(inputStream, Message.class);
                resultFlowable = this.buildFlowable(flowable, publisher, runContext);

                count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
            }

        } else if (this.from instanceof List) {
            flowable = Flux
                .fromArray(((List<Object>) this.from).toArray())
                .map(o -> JacksonMapper.toMap(o, Message.class));

            resultFlowable = this.buildFlowable(flowable, publisher, runContext);

            count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
        } else {
            var msg = JacksonMapper.toMap(this.from, Message.class);
            publisher.publish(msg.to(runContext, runContext.render(this.serdeType).as(SerdeType.class).orElseThrow()));
        }

        publisher.shutdown();

        // metrics
        runContext.metric(Counter.of("records", count, "topic", runContext.render(this.getTopic()).as(String.class).orElseThrow()));

        return Output.builder()
            .messagesCount(count)
        .build();
    }

    private Flux<Integer> buildFlowable(Flux<Message> flowable, Publisher publisher, RunContext runContext) throws Exception {
        return flowable
            .map(throwFunction(message -> {
                publisher.publish(message.to(runContext, runContext.render(this.serdeType).as(SerdeType.class).orElseThrow()));
                return 1;
            }));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of published messages.")
        private final Integer messagesCount;
    }
}
