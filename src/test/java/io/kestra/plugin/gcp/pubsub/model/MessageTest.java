package io.kestra.plugin.gcp.pubsub.model;

import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Data;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@KestraTest
class MessageTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void stringSerdePreservesRawByteArrayPayloadFromIonStorage() throws Exception {
        var expected = new byte[] { 0x00, 0x01, 0x7f, (byte) 0x80, (byte) 0xff };
        var runContext = runContextFactory.of();
        var file = runContext.workingDir().createTempFile(".ion");
        try (var output = Files.newOutputStream(file)) {
            FileSerde.write(output, Message.builder().data(expected).build());
        }

        URI uri;
        try (var input = new FileInputStream(file.toFile())) {
            uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), input);
        }

        var messages = Data.from(uri.toString())
            .readAs(runContext, Message.class, map -> JacksonMapper.toMap(map, Message.class))
            .collectList()
            .block();

        assertArrayEquals(expected, (byte[]) messages.getFirst().getData());
        assertArrayEquals(expected, messages.getFirst().to(runContext, SerdeType.STRING).getData().toByteArray());
    }
}
