package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_fetch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
                .collection("persons")
                .field("lastname")
                .value("Doe")
                .build();

        // create something to list
        try(var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John","lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane","lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles","lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try(var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void run_fetch_notEqualTo_withOrderBy() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
                .collection("persons")
                .field("lastname")
                .value("Doe")
                .queryOperator(Query.QueryOperator.NOT_EQUAL_TO)
                .orderBy("firstname")
                .build();

        // create something to list
        try(var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John","lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane","lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles","lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try(var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void run_stored() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
                .collection("persons")
                .field("lastname")
                .value("Doe")
                .store(true)
                .build();

        // create something to list
        try(var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John","lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane","lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles","lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));

        // clear the collection
        try(var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }
}