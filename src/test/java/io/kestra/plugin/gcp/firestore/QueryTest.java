package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.Rethrow;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwSupplier;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void runFetch() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .filters(List.of(
                Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build())
            )
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        // create something to list
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection("persons");
            collection.document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
            collection.document("2").set(Map.of("firstname", "Jane", "lastname", "Doe")).get();
            collection.document("3").set(Map.of("firstname", "Charles", "lastname", "Baudelaire")).get();
        }

        Await.until(
            throwSupplier(() -> {
                try (var firestore = query.connection(runContext)) {
                    return firestore.collection("persons").get().get().size() == 3;
                }
            }),
            Duration.ofMillis(100),
            Duration.ofSeconds(3)
        );

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
    void runFetchMultipleWhere() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .filters(List.of(
                Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build(),
                Query.Filter.builder().field(Property.ofValue("firstname")).value(Property.ofValue("Jane")).build())
            )
            .fetchType(Property.ofValue(FetchType.FETCH))
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
    void runFetchNoWhere() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .fetchType(Property.ofValue(FetchType.FETCH))
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
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .filters(List.of(
                Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).operator(Property.ofValue(Query.QueryOperator.NOT_EQUAL_TO)).build())
            )
            .orderBy(Property.ofValue("firstname"))
            .fetchType(Property.ofValue(FetchType.FETCH))
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
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .filters(List.of(
                Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build())
            )
            .fetchType(Property.ofValue(FetchType.STORE))
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
