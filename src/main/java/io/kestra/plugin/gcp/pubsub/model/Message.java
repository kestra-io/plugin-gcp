package io.kestra.plugin.gcp.pubsub.model;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@Getter
@Builder
@Jacksonized
public class Message {

    @Schema(title = "The message data, must be base64 encoded")
    @PluginProperty(dynamic = true)
    private String data;

    @Schema(title = "The message attribute map")
    @PluginProperty(dynamic = true)
    private Map<String, String> attributes;

    @Schema(title = "The message identifier")
    @PluginProperty(dynamic = true)
    private String messageId;

    @Schema(title = "The message ordering key")
    @PluginProperty(dynamic = true)
    private String orderingKey;

    public PubsubMessage to(RunContext runContext) throws IllegalVariableEvaluationException {
        var builder =  PubsubMessage.newBuilder();
        if(data != null) {
            builder.setData(ByteString.copyFrom(runContext.render(data).getBytes()));
        }
        if(attributes != null && !attributes.isEmpty()) {
            attributes.forEach(throwBiConsumer((key, value) -> builder.putAttributes(runContext.render(key), runContext.render(value))));
        }
        if(messageId != null) {
            builder.setMessageId(runContext.render(messageId));
        }
        if(orderingKey != null) {
            builder.setOrderingKey(runContext.render(orderingKey));
        }
        return builder.build();
    }

    public static Message of(PubsubMessage message) {
        return Message.builder()
            .messageId(message.getMessageId())
            .data(message.getData().toString())
            .attributes(message.getAttributesMap())
            .orderingKey(message.getOrderingKey())
            .build();
    }
}
