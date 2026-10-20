package io.kestra.plugin.gcp.firestore;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

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
@Testcontainers
class DeleteTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void run() throws Exception {
        String collection = "persons-" + UUID.randomUUID();
        String documentId = UUID.randomUUID().toString();
        Map<String, Object> expected = Map.of(
            "firstname", "John",
            "lastname", "Doe"
        );

        try (var emulatorScope = AbstractFirestore.useEmulatorEndpoint(FLOCI_GCP.getEmulatorHost())) {
            var runContext = runContextFactory.of();

            var delete = Delete.builder()
                .projectId(Property.ofValue(project))
                .collection(Property.ofValue(collection))
                .childPath(Property.ofValue(documentId))
                .build();

            try (var firestore = delete.connection(runContext)) {
                var document = firestore.collection(collection).document(documentId);

                try {
                    document.set(expected).get(30, TimeUnit.SECONDS);

                    var beforeDelete = document.get().get(30, TimeUnit.SECONDS);
                    assertThat(beforeDelete.exists(), is(true));
                    assertThat(beforeDelete.getData(), is(equalTo(expected)));

                    var output = delete.run(runContext);
                    assertThat(output.getUpdatedTime(), is(notNullValue()));

                    var afterDelete = document.get().get(30, TimeUnit.SECONDS);
                    assertThat(afterDelete.exists(), is(false));
                } finally {
                    document.delete().get(30, TimeUnit.SECONDS);
                }
            }
        }
    }
}
