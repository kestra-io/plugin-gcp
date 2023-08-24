package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class PublishThenConsumeTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.tasks.pubsub.project}")
    private String project;

    @Test
    void runWithList() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .projectId(project)
            .topic("test-topic")
            .from(
                List.of(
                    Message.builder().data("Hello World").build(),
                    Message.builder().attributes(Map.of("key", "value")).build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));

        var consume = Consume.builder()
            .projectId(project)
            .topic("test-topic")
            .subscription("test-subscription")
            .maxRecords(2)
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runWithJson() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .projectId(project)
            .topic("test-topic")
            .serdeType(SerdeType.JSON)
            .from(
                List.of(
                    Message.builder().data("""
                        {"hello": "world"}""").build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(1));

        var consume = Consume.builder()
            .projectId(project)
            .topic("test-topic")
            .serdeType(SerdeType.JSON)
            .subscription("test-subscription")
            .maxRecords(1)
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(1));
    }

    @Test
    void runWithFile() throws Exception {
        var runContext = runContextFactory.of();
        var uri = createTestFile(runContext);

        var publish = Publish.builder()
            .projectId(project)
            .topic("test-topic")
            .from(uri.toString())
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));


        var consume = Consume.builder()
            .projectId(project)
            .topic("test-topic")
            .subscription("test-subscription")
            .maxRecords(2)
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    private URI createTestFile(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);

        FileSerde.write(output,
            Message.builder().data("Hello World".getBytes()).build());
        FileSerde.write(output,
            Message.builder().attributes(Map.of("key", "value")).build());
        return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }
}