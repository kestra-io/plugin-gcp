package io.kestra.plugin.gcp.dataproc.batches;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

@KestraTest
class AbstractBatchExecutionConfigTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldSetEveryNetworkTag() throws Exception {
        var task = PySparkSubmit.builder()
            .execution(
                AbstractBatch.ExecutionConfiguration.builder()
                    .networkTags(Property.ofValue(List.of("dataproc", "etl")))
                    .build()
            )
            .build();

        var config = task.buildExecutionConfig(runContextFactory.of());

        assertThat(config.getNetworkTagsList(), contains("dataproc", "etl"));
    }

    @Test
    void shouldRenderNetworkTagsFromAnExpression() throws Exception {
        var task = PySparkSubmit.builder()
            .execution(
                AbstractBatch.ExecutionConfiguration.builder()
                    .networkTags(Property.ofExpression("{{ tags }}"))
                    .build()
            )
            .build();

        var config = task.buildExecutionConfig(runContextFactory.of(Map.of("tags", List.of("dataproc", "etl"))));

        assertThat(config.getNetworkTagsList(), contains("dataproc", "etl"));
    }

    @Test
    void shouldNotSetNetworkTagsWhenNoneAreGiven() throws Exception {
        var task = PySparkSubmit.builder()
            .execution(AbstractBatch.ExecutionConfiguration.builder().build())
            .build();

        var config = task.buildExecutionConfig(runContextFactory.of());

        assertThat(config.getNetworkTagsList(), empty());
    }
}
