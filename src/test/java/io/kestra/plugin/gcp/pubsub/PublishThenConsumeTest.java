package io.kestra.plugin.gcp.pubsub;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
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

@KestraTest
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
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
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
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .subscription(Property.ofValue("test-subscription"))
            .maxRecords(Property.ofValue(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runWithJson() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .serdeType(Property.ofValue(SerdeType.JSON))
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
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .subscription(Property.ofValue("test-subscription"))
            .maxRecords(Property.ofValue(1))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(1));
    }

    @Test
    void runWithFile() throws Exception {
        var runContext = runContextFactory.of();
        var uri = createTestFile(runContext);

        var publish = Publish.builder()
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .from(uri.toString())
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(2));


        var consume = Consume.builder()
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .subscription(Property.ofValue("test-subscription"))
            .maxRecords(Property.ofValue(2))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(2));
    }

    @Test
    void runWithMessagesWithOrderingKey() throws Exception {
        var runContext = runContextFactory.of();

        var publish = Publish.builder()
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .from(
                List.of(
                    Message.builder()
                        .data("first message")
                        .orderingKey("key-1")
                        .attributes(Map.of("sequence", "1"))
                        .build(),
                    Message.builder()
                        .data("second message")
                        .orderingKey("key-1")
                        .attributes(Map.of("sequence", "2"))
                        .build(),
                    Message.builder()
                        .orderingKey("key-2")
                        .attributes(Map.of("sequence", "3"))
                        .build(),
                    Message.builder()
                        .data("message without ordering key")
                        .attributes(Map.of("sequence", "4"))
                        .build()
                )
            )
            .build();

        var publishOutput = publish.run(runContext);
        assertThat(publishOutput.getMessagesCount(), is(4));

        var consume = Consume.builder()
            .projectId(Property.ofValue(project))
            .topic(Property.ofValue("test-topic"))
            .subscription(Property.ofValue("test-subscription"))
            .maxRecords(Property.ofValue(4))
            .build();

        var consumeOutput = consume.run(runContextFactory.of());
        assertThat(consumeOutput.getCount(), is(4));
    }

    private URI createTestFile(RunContext runContext) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        OutputStream output = new FileOutputStream(tempFile);

        FileSerde.write(output,
            Message.builder().data("Hello World".getBytes()).build());
        FileSerde.write(output,
            Message.builder().attributes(Map.of("key", "value")).build());
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    @BeforeEach
    void cleanTopic() {
        try {
            Consume.builder()
                .projectId(Property.ofValue(project))
                .topic(Property.ofValue("test-topic"))
                .subscription(Property.ofValue("test-subscription"))
                .maxRecords(Property.ofValue(50))
                .build()
                .run(runContextFactory.of());
        } catch (Exception ignored) {}
    }
}
