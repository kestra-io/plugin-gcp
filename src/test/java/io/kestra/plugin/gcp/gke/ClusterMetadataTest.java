package io.kestra.plugin.gcp.gke;

import com.google.common.collect.ImmutableMap;
import com.google.container.v1.Cluster;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.bigquery.Load;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@KestraTest
class ClusterMetadataTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        ClusterMetadata task = spy(ClusterMetadata.builder()
            .id(ClusterMetadataTest.class.getSimpleName())
            .type(Load.class.getName())
            .clusterZone(Property.of("my-zone"))
            .clusterId(Property.of("my-cluster"))
            .clusterProjectId(Property.of("my-project"))
            .build());

        doReturn(Cluster.newBuilder().setName("my-cluster").build())
            .when(task)
            .fetch(any());

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        ClusterMetadata.Output run = task.run(runContext);

        assertThat(run.getName(), is("my-cluster"));
    }
}
