package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContextFactory;
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

    @Test
    void run_map() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
                .collection("persons")
                .childPath("1")
                .document(Map.of("firstname", "John",
                        "lastname", "Doe"))
                .build();

        var output = set.run(runContext);

        assertThat(output.getUpdateTime(), is(notNullValue()));

        // clear the collection
        try(var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void run_string() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
                .collection("persons")
                .childPath("2")
                .document("{\"firstname\":\"Jane\",\"lastname\":\"Doe\"}")
                .build();

        var output = set.run(runContext);

        assertThat(output.getUpdateTime(), is(notNullValue()));

        // clear the collection
        try(var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void run_null() throws Exception {
        var runContext = runContextFactory.of();

        var set = Set.builder()
                .collection("persons")
                .childPath("3")
                .document(null)
                .build();

        var output = set.run(runContext);

        assertThat(output.getUpdateTime(), is(notNullValue()));

        // clear the collection
        try(var firestore = set.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }
}