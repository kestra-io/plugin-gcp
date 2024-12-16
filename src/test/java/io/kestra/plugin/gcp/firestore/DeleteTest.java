package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var delete = Delete.builder()
            .projectId(Property.of(project))
            .collection(Property.of("persons"))
            .childPath(Property.of("1"))
            .build();

        // create something to delete
        try (var firestore = delete.connection(runContext)) {
            firestore.collection("persons")
                .document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
        }

        var output = delete.run(runContext);

        assertThat(output.getUpdatedTime(), is(notNullValue()));
    }
}
