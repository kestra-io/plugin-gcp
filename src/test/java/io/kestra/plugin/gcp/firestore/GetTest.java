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
import static org.hamcrest.Matchers.*;

@KestraTest
class GetTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void run() throws Exception {
        String collection = "persons-" + UUID.randomUUID();
        String documentId = UUID.randomUUID().toString();

        var runContext = runContextFactory.of();

        var get = FirestoreTestConnections.spy(
            Get.builder()
                .projectId(Property.ofValue(project))
                .collection(Property.ofValue(collection))
                .childPath(Property.ofValue(documentId))
                .build(),
            FLOCI_GCP.getEmulatorHost()
        );

        try (var firestore = get.connection(runContext)) {
            try {
                firestore.collection(collection)
                    .document(documentId)
                    .set(Map.of("firstname", "John", "lastname", "Doe"))
                    .get(30, TimeUnit.SECONDS);

                var output = get.run(runContext);

                assertThat(output.getRow(), is(notNullValue()));
                assertThat(output.getRow().get("firstname"), is(equalTo("John")));
                assertThat(output.getRow().get("lastname"), is(equalTo("Doe")));
            } finally {
                firestore.collection(collection)
                    .document(documentId)
                    .delete()
                    .get(30, TimeUnit.SECONDS);
            }
        }
    }
}
