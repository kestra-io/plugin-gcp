package org.kestra.task.gcp.gke;

import com.google.auth.oauth2.AccessToken;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.bigquery.Load;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class AccessTokenTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void getMetas() throws Exception {

        GcpAccessToken task = GcpAccessToken.builder()
            .id(AccessTokenTest.class.getSimpleName())
            .type(Load.class.getName())
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        GcpAccessToken.Output run = task.run(runContext);

        AccessToken accessToken = run.getAccessToken();
        assertThat(accessToken, notNullValue());

    }

}
