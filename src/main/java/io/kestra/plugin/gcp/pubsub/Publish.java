package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish messages to Pub/Sub",
    description = "Publishes one or more messages to a topic. Supports STRING/JSON/AVRO serde and optional ordering keys."
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
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records published to the Pub/Sub topic."
        )
    }
)
public class Publish extends AbstractPubSub implements RunnableTask<Publish.Output>, io.kestra.core.models.property.Data.From {
    @NotNull
    @Schema(
        title = io.kestra.core.models.property.Data.From.TITLE,
        description = io.kestra.core.models.property.Data.From.DESCRIPTION,
        anyOf = {String.class, Message[].class, Message.class}
    )
    private Object from;

    @Builder.Default
    @NotNull
    @Schema(
        title = "Serde type",
        description = "Serializer/deserializer for message payloads; defaults to STRING"
    )
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        boolean hasOrderingKeys = checkForOrderingKeys(runContext);

        var publisher = this.createPublisher(
            AbstractPubSub.PublisherOptions.builder()
                .runContext(runContext)
                .enableMessageOrdering(hasOrderingKeys)
                .build()
        );

        Integer count = io.kestra.core.models.property.Data.from(from)
            .readAs(runContext, Message.class, map -> JacksonMapper.toMap(map, Message.class))
            .map(throwFunction(message -> {
                publisher.publish(message.to(runContext, runContext.render(this.serdeType).as(SerdeType.class).orElseThrow()));
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional()
            .orElse(0);

        publisher.shutdown();

        // metrics
        runContext.metric(Counter.of("records", count, "topic", runContext.render(this.getTopic()).as(String.class).orElseThrow()));

        return Output.builder()
            .messagesCount(count)
        .build();
    }

    private boolean checkForOrderingKeys(RunContext runContext) {
        try {
            return io.kestra.core.models.property.Data.from(from)
                .readAs(runContext, Message.class, map -> JacksonMapper.toMap(map, Message.class))
                .any(message -> message.getOrderingKey() != null && !message.getOrderingKey().trim().isEmpty())
                .blockOptional()
                .orElse(false);
        } catch (Exception e) {
            runContext.logger().info("Failed to parse messages while checking for ordering keys. ",e);
            return false;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Published message count")
        private final Integer messagesCount;
    }
}
