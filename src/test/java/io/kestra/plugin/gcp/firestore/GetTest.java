package io.kestra.plugin.gcp.firestore;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.firestore.project}")
    private String project;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var get = Get.builder()
            .projectId(Property.ofValue(project))
            .collection(Property.ofValue("persons"))
            .childPath(Property.ofValue("1"))
            .build();

        // create something to get
        try (var firestore = get.connection(runContext)) {
            firestore.collection("persons")
                .document("1").set(Map.of("firstname", "John", "lastname", "Doe")).get();
        }

        var output = get.run(runContext);

        assertThat(output.getRow(), is(notNullValue()));
        assertThat(output.getRow().get("firstname"), is(equalTo("John")));
        assertThat(output.getRow().get("lastname"), is(equalTo("Doe")));
    }
}