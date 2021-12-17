package io.kestra.plugin.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.bigquery.Load;

import jakarta.inject.Inject;

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
