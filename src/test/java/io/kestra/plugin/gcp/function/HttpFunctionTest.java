package io.kestra.plugin.gcp.function;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class HttpFunctionTest {
    private static final String GCP_FUNCTION_TEST_URL = "";
    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("Disabled with CI/CD, to run the test provide a GCP Function URL")
    @EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
    @Test
    void testAzureFunctionWithStringOutput() throws Exception {
        HttpFunction httpTrigger = HttpFunction.builder()
            .url(Property.ofValue(GCP_FUNCTION_TEST_URL + "?firstName=Bryan&name=Smith"))
            .httpMethod(Property.ofValue("GET"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpFunction.Output functionOutput = httpTrigger.run(runContext);

        JsonNode objectResult = (JsonNode) functionOutput.getResponseBody();
        assertThat(objectResult.get("firstName").asText(), is("Bryan"));
        assertThat(objectResult.get("name").asText(), is("Smith"));
    }

    @Test
    void readIdleTimeoutDefaultsToMaxDurationDefault() throws Exception {
        HttpFunction task = HttpFunction.builder()
            .url(Property.ofValue("https://my-function.europe-west9.run.app"))
            .httpMethod(Property.ofValue("GET"))
            .build();

        assertThat(readIdleTimeout(task, Collections.emptyMap()), is(Duration.ofMinutes(60)));
    }

    @Test
    void readIdleTimeoutUsesMaxDuration() throws Exception {
        HttpFunction task = HttpFunction.builder()
            .url(Property.ofValue("https://my-function.europe-west9.run.app"))
            .httpMethod(Property.ofValue("GET"))
            .maxDuration(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        assertThat(readIdleTimeout(task, Collections.emptyMap()), is(Duration.ofMinutes(5)));
    }

    @Test
    void readIdleTimeoutUsesRenderedMaxDuration() throws Exception {
        HttpFunction task = HttpFunction.builder()
            .url(Property.ofValue("https://my-function.europe-west9.run.app"))
            .httpMethod(Property.ofValue("GET"))
            .maxDuration(Property.ofExpression("{{ timeout }}"))
            .build();

        assertThat(readIdleTimeout(task, Map.of("timeout", "PT2M30S")), is(Duration.ofSeconds(150)));
    }

    private Duration readIdleTimeout(HttpFunction task, Map<String, Object> variables) throws Exception {
        RunContext runContext = runContextFactory.of(variables);
        HttpConfiguration configuration = task.httpConfiguration(runContext);

        return runContext.render(configuration.getTimeout().getReadIdleTimeout()).as(Duration.class).orElseThrow();
    }
}
