package io.kestra.plugin.gcp.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
                .projectId(Property.of(projectId))
                .serviceAccount(Property.of(serviceAccount))
                .env(new Property<>(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}")))
                .commands(Property.of(List.of(
                        "echo \"::{\\\"outputs\\\":{" +
                                "\\\"appCredentials\\\":\\\"$(cat $GOOGLE_APPLICATION_CREDENTIALS)\\\"," +
                                "\\\"cliCredentials\\\":\\\"$(cat $CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE)\\\"," +
                                "\\\"projectId\\\":\\\"$CLOUDSDK_CORE_PROJECT\\\"," +
                                "\\\"customEnv\\\":\\\"$"+envKey+"\\\"" +
                                "}}::\"",
                        "gcloud version"
                )))
                .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("appCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("cliCredentials"), is(serviceAccount));
        assertThat(runOutput.getVars().get("projectId"), is(projectId));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
    }
}
