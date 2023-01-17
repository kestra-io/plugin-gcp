package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var list = List.builder()
            .projectId(project)
            .collection("persons")
            .build();

        // create something to list
        try (var firestore = list.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = list.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows().size(), is(3));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try (var firestore = list.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runStored() throws Exception {
        var runContext = runContextFactory.of();

        var list = List.builder()
            .projectId(project)
            .collection("persons")
            .store(true)
            .build();

        // create something to list
        try (var firestore = list.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = list.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));

        // clear the collection
        try (var firestore = list.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }
}
