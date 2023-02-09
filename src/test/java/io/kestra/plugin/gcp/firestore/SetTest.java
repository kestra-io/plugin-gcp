package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
class SetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void runMap() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
            .projectId(project)
            .collection("persons")
            .childPath("1")
            .document(
                Map.of(
                    "firstname", "John",
                    "lastname", "Doe"
                )
            )
            .build();

        var output = set.run(runContext);

        assertThat(output.getUpdatedTime(), is(notNullValue()));

        // clear the collection
        try (var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runString() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
            .projectId(project)
            .collection("persons")
            .childPath("2")
            .document("{\"firstname\":\"Jane\",\"lastname\":\"Doe\"}")
            .build();

        var output = set.run(runContext);

        assertThat(output.getUpdatedTime(), is(notNullValue()));

        // clear the collection
        try (var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runNull() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
            .projectId(project)
            .collection("persons")
            .childPath("3")
            .document(null)
            .build();

        var output = set.run(runContext);

        assertThat(output.getUpdatedTime(), is(notNullValue()));

        // clear the collection
        try (var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }
}
