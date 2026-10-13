package io.kestra.plugin.gcp.bigquery;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ExtractToGcsConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRenderCompressionWithoutDoubleEvaluation() throws Exception {
        var task = ExtractToGcs.builder()
            .id("extract")
            .type(ExtractToGcs.class.getName())
            .sourceTable(Property.ofValue("my-project.my_dataset.my_table"))
            .destinationUris(Property.ofValue(List.of("gs://my-bucket/file.csv")))
            .compression(Property.ofExpression("{{ inputs.comp }}"))
            .build();

        var runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            Map.of("comp", "{{ 'GZIP' }}")
        );

        var configuration = task.buildExtractJob(runContext);
        assertThat(configuration.getCompression(), is("{{ 'GZIP' }}"));
    }

    @Test
    void shouldRenderStandardCompressionExpression() throws Exception {
        var task = ExtractToGcs.builder()
            .id("extract")
            .type(ExtractToGcs.class.getName())
            .sourceTable(Property.ofValue("my-project.my_dataset.my_table"))
            .destinationUris(Property.ofValue(List.of("gs://my-bucket/file.csv")))
            .compression(Property.ofExpression("{{ 'GZIP' }}"))
            .build();

        var runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            Map.of()
        );

        var configuration = task.buildExtractJob(runContext);
        assertThat(configuration.getCompression(), is("GZIP"));
    }
}
