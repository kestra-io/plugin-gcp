package io.kestra.plugin.gcp.pubsub.model;

import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.PubsubMessage;

import io.kestra.core.runners.RunContext;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;

class MessageTest {
    @Test
    void stringSerdePreservesRawByteArrayPayload() throws Exception {
        byte[] expected = new byte[] { 0x00, 0x01, 0x7f, (byte) 0x80, (byte) 0xff };
        RunContext runContext = mock(RunContext.class);

        PubsubMessage publishedMessage = Message.builder()
            .data(expected)
            .build()
            .to(runContext, SerdeType.STRING);

        assertArrayEquals(expected, publishedMessage.getData().toByteArray());
    }
}
