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

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@Getter
@Builder
@Jacksonized
public class Message {

    @Schema(
        title = "The message data, must be a string if serde type is 'STRING', otherwise a JSON object",
        description = "If it's a string, it can be a dynamic property otherwise not."
    )
    @PluginProperty(dynamic = true)
    private Object data;

    @Schema(title = "The message attributes map")
    @PluginProperty(dynamic = true)
    private Map<String, String> attributes;

    @Schema(title = "The message identifier")
    @PluginProperty(dynamic = true)
    private String messageId;

    @Schema(title = "The message ordering key")
    @PluginProperty(dynamic = true)
    private String orderingKey;

    public PubsubMessage to(RunContext runContext, SerdeType serdeType) throws IllegalVariableEvaluationException, IOException {
        var builder =  PubsubMessage.newBuilder();
        if(data != null) {
            byte[] serializedData;
            if (data instanceof String dataStr) {
                var rendered = runContext.render(dataStr);
                serializedData = rendered.getBytes();
            } else {
                serializedData = serdeType.serialize(data);
            }
            builder.setData(ByteString.copyFrom(Base64.getEncoder().encode(serializedData)));
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

    public static Message of(PubsubMessage message, SerdeType serdeType) throws IOException {
        var builder = Message.builder()
            .messageId(message.getMessageId())
            .attributes(message.getAttributesMap())
            .orderingKey(message.getOrderingKey());

        if (message.getData() != null) {
            var decodedData = Base64.getDecoder().decode(message.getData().toByteArray());
            builder.data(serdeType.deserialize(decodedData));
        }

        return builder.build();
    }
}
