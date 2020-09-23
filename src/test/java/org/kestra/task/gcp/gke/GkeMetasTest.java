package org.kestra.task.gcp.gke;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import com.google.container.v1.Cluster;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.bigquery.AbstractLoad;
import org.kestra.task.gcp.bigquery.Load;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class GkeMetasTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void getMetas() throws Exception {

        GkeMetas task = GkeMetas.builder()
            .id(GkeMetasTest.class.getSimpleName())
            .type(Load.class.getName())
            .zone("europe-west1-c")
            .clusterId("my-cluster")
            .projectId("my-project")
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        GkeMetas.Output run = task.run(runContext);

        GkeMetas.ClusterMetaData clusterMetaData = run.getClusterMetaData();
        assertThat(clusterMetaData, is("test"));

    }

}
