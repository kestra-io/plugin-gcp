package io.kestra.plugin.gcp.pubsub.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;

public enum SerdeType {
    STRING,
    JSON;

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson(false);

    public Object deserialize(byte[] message) throws IOException {
        if (this == SerdeType.JSON) {
            return OBJECT_MAPPER.readValue(message, Object.class);
        } else {
            return message;
        }
    }

    public byte[] serialize(Object message) throws IOException {
        if (this == SerdeType.JSON) {
            return OBJECT_MAPPER.writeValueAsBytes(message);
        } else {
            return message.toString().getBytes();
        }
    }
}
