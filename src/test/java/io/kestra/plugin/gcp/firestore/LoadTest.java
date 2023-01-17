package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class LoadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();
        var from = createTestFile(runContext);

        var load = Load.builder()
            .projectId(project)
            .from(from.toString())
            .collection("persons")
            .build();
        var output = load.run(runContext);

        assertThat(output.getSize(), is(3L));

        // clear the collection
        try (var firestore = load.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runWithChildPathKey() throws Exception {
        var runContext = runContextFactory.of();
        var from = createTestFile(runContext);

        var load = Load.builder()
            .projectId(project)
            .from(from.toString())
            .collection("persons")
            .keyPath("id")
            .build();
        var output = load.run(runContext);

        assertThat(output.getSize(), is(3L));

        // clear the collection
        try (var firestore = load.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    private URI createTestFile(RunContext runContext) throws Exception {
        var tempFile = runContext.tempFile(".ion").toFile();
        try (var outputStream = new FileOutputStream(tempFile)) {
            var person1 = Map.of(
                "id", "1",
                "firstname", "John",
                "lastname", "Doe"
            );
            FileSerde.write(outputStream, person1);
            var person2 = Map.of(
                "id", "2",
                "firstname", "Jane",
                "lastname", "Doe"
            );
            FileSerde.write(outputStream, person2);
            var person3 = Map.of(
                "id", "3",
                "firstname", "Charles",
                "lastname", "Baudelaire"
            );
            FileSerde.write(outputStream, person3);
            return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
        }
    }
}
