package org.kestra.task.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.utils.TestsUtils;
import org.kestra.task.gcp.bigquery.Load;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
class OauthAccessTokenTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        OauthAccessToken task = OauthAccessToken.builder()
            .id(OauthAccessTokenTest.class.getSimpleName())
            .type(Load.class.getName())
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        OauthAccessToken.Output run = task.run(runContext);

        AccessToken accessToken = run.getAccessToken();
        assertThat(accessToken, notNullValue());
    }
}
