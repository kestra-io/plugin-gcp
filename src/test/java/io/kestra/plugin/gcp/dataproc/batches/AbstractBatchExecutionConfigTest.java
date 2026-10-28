package io.kestra.plugin.gcp.dataproc.batches;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.dataproc.v1.ExecutionConfig;

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
        PySparkSubmit task = PySparkSubmit.builder()
            .execution(
                AbstractBatch.ExecutionConfiguration.builder()
                    .networkTags(Property.ofValue(List.of("dataproc", "etl")))
                    .build()
            )
            .build();

        ExecutionConfig config = task.buildExecutionConfig(runContextFactory.of());

        assertThat(config.getNetworkTagsList(), contains("dataproc", "etl"));
    }

    @Test
    void shouldNotSetNetworkTagsWhenNoneAreGiven() throws Exception {
        PySparkSubmit task = PySparkSubmit.builder()
            .execution(AbstractBatch.ExecutionConfiguration.builder().build())
            .build();

        ExecutionConfig config = task.buildExecutionConfig(runContextFactory.of());

        assertThat(config.getNetworkTagsList(), empty());
    }
}
