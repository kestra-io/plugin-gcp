package io.kestra.plugin.gcp.function;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class HttpFunctionTest {
    private static final String GCP_FUNCTION_TEST_URL = "";
    @Inject
    private RunContextFactory runContextFactory;

    @Disabled("Disabled with CI/CD, to run the test provide a GCP Function URL")
    @Test
    void testAzureFunctionWithStringOutput() throws Exception {
        HttpFunction httpTrigger = HttpFunction.builder()
            .url(Property.of(GCP_FUNCTION_TEST_URL + "?firstName=Bryan&name=Smith"))
            .httpMethod(Property.of("GET"))
            .build();

        RunContext runContext = runContextFactory.of(Collections.emptyMap());

        HttpFunction.Output functionOutput = httpTrigger.run(runContext);

        JsonNode objectResult = (JsonNode) functionOutput.getResponseBody();
        assertThat(objectResult.get("firstName").asText(), is("Bryan"));
        assertThat(objectResult.get("name").asText(), is("Smith"));
    }
}