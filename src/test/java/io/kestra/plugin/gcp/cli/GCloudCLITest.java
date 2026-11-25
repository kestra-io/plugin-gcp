package io.kestra.plugin.gcp.cli;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
class GCloudCLITest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        String serviceAccount = "someServiceAccount";
        String projectId = "someProjectId";
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";
        GCloudCLI execute = GCloudCLI.builder()
            .id(IdUtils.create())
            .type(GCloudCLI.class.getName())
            .projectId(Property.ofValue(projectId))
            .serviceAccount(Property.ofValue(serviceAccount))
            .env(Property.ofValue(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}")))
            .commands(
                Property.ofValue(
                    List.of(
                        "echo \"::{\\\"outputs\\\":{" +
                            "\\\"appCredentials\\\":\\\"$(cat $GOOGLE_APPLICATION_CREDENTIALS)\\\"," +
                            "\\\"cliCredentials\\\":\\\"$(cat $CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE)\\\"," +
                            "\\\"projectId\\\":\\\"$CLOUDSDK_CORE_PROJECT\\\"," +
                            "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                            "}}::\"",
                        "gcloud version"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("appCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("cliCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("projectId"), is(projectId));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
    }

    @Test
    void userEnvOverridesTheProjectAndTheKeyStillReachesTheProcess() throws Exception {
        var task = GCloudCLI.builder()
            .id(IdUtils.create())
            .type(GCloudCLI.class.getName())
            .taskRunner(Process.instance())
            .projectId(Property.ofValue("from-project-id"))
            .serviceAccount(Property.ofValue("someServiceAccount"))
            .env(Property.ofValue(Map.of("CLOUDSDK_CORE_PROJECT", "from-env")))
            .commands(
                Property.ofValue(
                    List.of(
                        "echo \"::{\\\"outputs\\\":{\\\"project\\\":\\\"$CLOUDSDK_CORE_PROJECT\\\",\\\"key\\\":\\\"$(cat $GOOGLE_APPLICATION_CREDENTIALS)\\\"}}::\""
                    )
                )
            )
            .build();

        var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getVars().get("project"), is("from-env"));
        assertThat(output.getVars().get("key"), is("someServiceAccount"));
    }

    @Test
    void failingLineDoesNotStopTheRunByDefault() throws Exception {
        var task = GCloudCLI.builder()
            .id(IdUtils.create())
            .type(GCloudCLI.class.getName())
            .taskRunner(Process.instance())
            .commands(Property.ofValue(List.of("false", "echo ok")))
            .build();

        assertThat(task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())).getExitCode(), is(0));
    }

    @Test
    void toStringDoesNotExposeEnvSecrets() {
        var task = GCloudCLI.builder()
            .id(IdUtils.create())
            .type(GCloudCLI.class.getName())
            .commands(Property.ofValue(List.of("gcloud version")))
            .serviceAccount(Property.ofValue("super-secret-key"))
            .env(Property.ofValue(Map.of("SECRET_KEY", "super-secret-value")))
            .build();

        assertThat(task.toString(), not(containsString("super-secret-value")));
        assertThat(task.toString(), not(containsString("super-secret-key")));
    }
}
