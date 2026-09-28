package io.kestra.plugin.gcp;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.AbstractTrigger;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Guards the no-code editor grouping of the GCP connection properties: they must all live in the
 * {@code connection} group, consistently with the shared kernel (plugin-gcp-lib) and plugin-ee-gcp.
 * Covers kernel inheritance ({@code AbstractTask}, {@code gcs.Trigger}, {@code pubsub}) as well as
 * the classes that redeclare these properties with their own annotations.
 */
@KestraTest
class ConnectionPropertyGroupsTest {
    private static final List<String> CONNECTION_PROPERTIES = List.of("projectId", "serviceAccount", "impersonatedServiceAccount", "scopes");

    @Inject
    private JsonSchemaGenerator jsonSchemaGenerator;

    static Stream<Arguments> classes() {
        return Stream.of(
            Arguments.of(Task.class, io.kestra.plugin.gcp.gcs.Upload.class),
            Arguments.of(Task.class, io.kestra.plugin.gcp.bigquery.Query.class),
            Arguments.of(Task.class, io.kestra.plugin.gcp.bigtable.ReadRows.class),
            Arguments.of(Task.class, io.kestra.plugin.gcp.spanner.Query.class),
            Arguments.of(Task.class, io.kestra.plugin.gcp.pubsub.Publish.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.gcs.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.bigquery.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.bigtable.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.dataflow.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.monitoring.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.pubsub.Trigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.pubsub.RealtimeTrigger.class),
            Arguments.of(AbstractTrigger.class, io.kestra.plugin.gcp.spanner.Trigger.class)
        );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @ParameterizedTest
    @MethodSource("classes")
    void connectionPropertiesAreGroupedUnderConnection(Class base, Class cls) {
        Map<String, Object> schema = jsonSchemaGenerator.properties(base, cls);
        Map<String, Map<String, Object>> properties = (Map<String, Map<String, Object>>) schema.get("properties");

        CONNECTION_PROPERTIES.stream()
            .filter(properties::containsKey)
            .forEach(name -> assertThat(cls.getSimpleName() + "." + name, properties.get(name).get("$group"), is("connection")));

        assertThat(properties.get("serviceAccount"), notNullValue());
        assertThat(cls.getSimpleName() + ".serviceAccount", properties.get("serviceAccount").get("$secret"), is(true));
    }
}
