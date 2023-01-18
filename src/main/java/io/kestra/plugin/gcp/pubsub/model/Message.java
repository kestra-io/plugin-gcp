package io.kestra.plugin.gcp.pubsub.model;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;

@Getter
@Builder
@Jacksonized
public class Message {

    @Schema(title = "The message data")
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

    public PubsubMessage to() {
        var builder =  PubsubMessage.newBuilder();
        if(data != null) {
            builder.setData(ByteString.copyFrom(data.getBytes()));
        }
        if(attributes != null && !attributes.isEmpty()) {
            attributes.forEach((key, value) -> builder.putAttributes(key, value));
        }
        if(messageId != null) {
            builder.setMessageId(messageId);
        }
        if(orderingKey != null) {
            builder.setOrderingKey(orderingKey);
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
