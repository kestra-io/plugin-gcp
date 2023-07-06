package io.kestra.plugin.gcp.cli;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class CliTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String serviceAccount = "someServiceAccount";
        String projectId = "someProjectId";
        Cli execute = Cli.builder()
                .id(IdUtils.create())
                .type(Cli.class.getName())
                .projectId(projectId)
                .serviceAccount(serviceAccount)
                .commands(List.of(
                        "echo \"::{\\\"outputs\\\":{" +
                                "\\\"appCredentials\\\":\\\"$(cat $GOOGLE_APPLICATION_CREDENTIALS)\\\"," +
                                "\\\"cliCredentials\\\":\\\"$(cat $CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE)\\\"," +
                                "\\\"projectId\\\":\\\"$CLOUDSDK_CORE_PROJECT\\\"" +
                                "}}::\"",
                        "gcloud version"
                ))
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("appCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("cliCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("projectId"), is(projectId));
    }
}
