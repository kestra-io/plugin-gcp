package io.kestra.plugin.gcp.firestore;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class SetTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runMap() throws Exception {
        Map<String, Object> expected = Map.of(
            "firstname", "John",
            "lastname", "Doe"
        );

        runAndVerify(expected, expected);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runString() throws Exception {
        runAndVerify(
            "{\"firstname\":\"Jane\",\"lastname\":\"Doe\"}",
            Map.of(
                "firstname", "Jane",
                "lastname", "Doe"
            )
        );
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void runNull() throws Exception {
        runAndVerify(null, Map.of());
    }

    private void runAndVerify(Object document, Map<String, Object> expectedFields) throws Exception {
        String collection = "persons-" + UUID.randomUUID();
        String documentId = UUID.randomUUID().toString();

        var runContext = runContextFactory.of();

        var set = FirestoreTestConnections.spy(
            Set.builder()
                .projectId(Property.ofValue(project))
                .collection(Property.ofValue(collection))
                .childPath(Property.ofValue(documentId))
                .document(document)
                .build(),
            FLOCI_GCP.getEmulatorHost()
        );

        try (var firestore = set.connection(runContext)) {
            try {
                var output = set.run(runContext);

                assertThat(output.getUpdatedTime(), is(notNullValue()));

                var snapshot = firestore.collection(collection)
                    .document(documentId)
                    .get()
                    .get(30, TimeUnit.SECONDS);

                assertThat(snapshot.exists(), is(true));
                assertThat(snapshot.getData(), is(equalTo(expectedFields)));
            } finally {
                firestore.collection(collection)
                    .document(documentId)
                    .delete()
                    .get(30, TimeUnit.SECONDS);
            }
        }
    }
}
