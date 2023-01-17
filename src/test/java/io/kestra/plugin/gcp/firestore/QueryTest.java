package io.kestra.plugin.gcp.firestore;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.StoreType;
import io.micronaut.context.annotation.Value;
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

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(project)
            .collection("persons")
            .field("lastname")
            .value("Doe")
            .storeType(StoreType.FETCH)
            .build();

        // create something to list
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows().size(), is(2));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try (var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runFetchNoWhere() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(project)
            .collection("persons")
            .storeType(StoreType.FETCH)
            .build();

        // create something to list
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows().size(), is(3));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try (var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runFetchNotEqualToWithOrderBy() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(project)
            .collection("persons")
            .field("lastname")
            .value("Doe")
            .queryOperator(Query.QueryOperator.NOT_EQUAL_TO)
            .orderBy("firstname")
            .storeType(StoreType.FETCH)
            .build();

        // create something to list
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getUri(), is(nullValue()));

        // clear the collection
        try (var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }

    @Test
    void runStored() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(project)
            .collection("persons")
            .field("lastname")
            .value("Doe")
            .storeType(StoreType.STORE)
            .build();

        // create something to list
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        var output = query.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getUri(), is(notNullValue()));

        // clear the collection
        try (var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, "persons");
        }
    }
}
