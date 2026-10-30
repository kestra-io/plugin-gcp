package io.kestra.plugin.gcp.firestore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest extends AbstractFlociGcpTest {
    private static final long OPERATION_TIMEOUT_SECONDS = 30;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runFetch() throws Exception {
        String collectionName = "persons_" + UUID.randomUUID();

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .filters(
                List.of(
                    Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build()
                )
            )
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);

            try (var firestore = query.connection(runContext)) {
                var seededCount = firestore.collection(collectionName)
                    .get()
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .size();
                assertThat(seededCount, is(people.size()));
            }

            var output = query.run(runContext);

            assertThat(output.getSize(), is(2L));
            assertThat(output.getRows().size(), is(2));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(output.getRows(), containsInAnyOrder(people.get(0), people.get(1)));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runFetchMultipleWhere() throws Exception {
        String collectionName = "persons_" + UUID.randomUUID();

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .filters(
                List.of(
                    Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build(),
                    Query.Filter.builder().field(Property.ofValue("firstname")).value(Property.ofValue("Jane")).build()
                )
            )
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);
            var output = query.run(runContext);

            assertThat(output.getSize(), is(1L));
            assertThat(output.getRows().size(), is(1));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(output.getRows(), contains(people.get(1)));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runFetchNoWhere() throws Exception {
        String collectionName = "persons_" + UUID.randomUUID();

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);
            var output = query.run(runContext);

            assertThat(output.getSize(), is(3L));
            assertThat(output.getRows().size(), is(3));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(output.getRows(), containsInAnyOrder(people.toArray()));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runFetchNotEqualToWithOrderBy() throws Exception {
        String collectionName = "persons_" + UUID.randomUUID();

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .filters(
                List.of(
                    Query.Filter.builder()
                        .field(Property.ofValue("lastname"))
                        .value(Property.ofValue("Doe"))
                        .operator(Property.ofValue(Query.QueryOperator.NOT_EQUAL_TO))
                        .build()
                )
            )
            .orderBy(Property.ofValue("firstname"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);
            var output = query.run(runContext);

            assertThat(output.getSize(), is(1L));
            assertThat(output.getRows().size(), is(1));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(output.getRows(), contains(people.get(2)));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runFetchOne() throws Exception {
        var collectionName = "persons_%s".formatted(UUID.randomUUID());

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .filters(
                List.of(
                    Query.Filter.builder().field(Property.ofValue("firstname")).value(Property.ofValue("John")).build()
                )
            )
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);
            var output = query.run(runContext);

            assertThat(output.getRow(), is(people.getFirst()));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runStored() throws Exception {
        String collectionName = "persons_" + UUID.randomUUID();

        var runContext = runContextFactory.of();
        var query = Query.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue(collectionName))
            .filters(
                List.of(
                    Query.Filter.builder().field(Property.ofValue("lastname")).value(Property.ofValue("Doe")).build()
                )
            )
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        query = FirestoreTestConnections.spy(query, FLOCI_GCP.getEmulatorHost());

        try {
            var people = seedPeople(query, runContext, collectionName);
            var output = query.run(runContext);

            assertThat(output.getSize(), is(2L));
            assertThat(output.getRows(), is(nullValue()));
            assertThat(output.getUri(), is(notNullValue()));

            List<Object> storedRows;
            try (var storedFile = runContext.storage().getFile(output.getUri())) {
                storedRows = FileSerde.readAll(storedFile)
                    .collectList()
                    .block(Duration.ofSeconds(OPERATION_TIMEOUT_SECONDS));
            }
            assertThat(storedRows, containsInAnyOrder(people.get(0), people.get(1)));
        } finally {
            clearCollection(query, runContext, collectionName);
        }
    }

    private List<Map<String, Object>> seedPeople(Query query, RunContext runContext, String collectionName) throws Exception {
        // Jane carries her own _id field, which must win over the document ID in the returned row.
        var documents = List.of(
            Map.entry("john", person("John", "Doe")),
            Map.entry("jane", Map.of("_id", "jane-own-id", "firstname", "Jane", "lastname", "Doe")),
            Map.entry("charles", person("Charles", "Baudelaire"))
        );

        var rows = new ArrayList<Map<String, Object>>(documents.size());
        try (var firestore = query.connection(runContext)) {
            var collection = firestore.collection(collectionName);
            for (var document : documents) {
                collection.document(document.getKey())
                    .set(document.getValue())
                    .get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                var row = new HashMap<String, Object>(document.getValue());
                row.putIfAbsent("_id", document.getKey());
                rows.add(row);
            }
        }

        return rows;
    }

    private void clearCollection(Query query, RunContext runContext, String collectionName) throws Exception {
        try (var firestore = query.connection(runContext)) {
            FirestoreTestUtil.clearCollection(firestore, collectionName);
        }
    }

    private static Map<String, Object> person(String firstname, String lastname) {
        return Map.of("firstname", firstname, "lastname", lastname);
    }
}
