package io.kestra.plugin.gcp.secrets;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import java.io.FileInputStream;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@MicronautTest
class GetSecretValueTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        GetSecretValue task = spy(GetSecretValue.builder()
                .name("TEST_SECRET")
                .version("latest")
                .projectId("kestra-392920")
                .build());

        try (InputStream inputStream = new FileInputStream("response.bin")) {
            AccessSecretVersionResponse responseMocked = AccessSecretVersionResponse.parseFrom(inputStream);

            // Assert mocked value is a correct instance of AccessSecretVersionResponse
            assertThat(responseMocked.getPayload().getData().toStringUtf8(), is("cookie"));

            doReturn(responseMocked)
                    .when(task)
                    .fetch(any());

            RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
            GetSecretValue.Output run = task.run(runContext);

            assertThat(run.getSecretValue(), is("cookie"));
        }
    }
}
