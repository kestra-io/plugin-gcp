package io.kestra.plugin.gcp.function;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class HttpFunctionUnitTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testMaxDurationConfiguration() throws Exception {
        RunContext runContext = runContextFactory.of();

        HttpFunction defaultFunction = HttpFunction.builder()
            .id("default-function")
            .type(HttpFunction.class.getName())
            .url(Property.ofValue("https://example.com"))
            .build();

        Duration defaultDuration = runContext.render(defaultFunction.getMaxDuration()).as(Duration.class).orElseThrow();
        assertThat(defaultDuration, is(Duration.ofMinutes(60)));

        HttpFunction customFunction = HttpFunction.builder()
            .id("custom-function")
            .type(HttpFunction.class.getName())
            .url(Property.ofValue("https://example.com"))
            .maxDuration(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        Duration customDuration = runContext.render(customFunction.getMaxDuration()).as(Duration.class).orElseThrow();
        assertThat(customDuration, is(Duration.ofMinutes(5)));
    }
}
