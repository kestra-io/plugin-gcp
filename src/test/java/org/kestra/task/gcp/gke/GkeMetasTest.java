package org.kestra.task.gcp.gke;

import com.google.common.collect.ImmutableMap;
import com.google.container.v1.Cluster;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.bigquery.Load;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@MicronautTest
class GkeMetasTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        GkeMetas task = spy(GkeMetas.builder()
            .id(GkeMetasTest.class.getSimpleName())
            .type(Load.class.getName())
            .zone("my-zone")
            .clusterId("my-cluster")
            .projectId("my-project")
            .build());

        doReturn(Cluster.newBuilder().setName("my-cluster").build())
            .when(task)
            .fetch(any());

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        GkeMetas.Output run = task.run(runContext);

        assertThat(run.getName(), is("my-cluster"));
    }
}
