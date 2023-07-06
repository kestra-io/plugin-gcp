package io.kestra.plugin.gcp.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Execute gcloud commands."
)
@Plugin(
        examples = {
                @Example(
                        title = "Create a cluster then list them using a service account",
                        code = {
                                "serviceAccount: \"{{ secret('gcp-sa') }}\"",
                                "commands:",
                                "  - gcloud container clusters create simple-cluster",
                                "  - gcloud container clusters list"
                        }
                ),
        }
)
public class Cli extends AbstractTask implements RunnableTask<ScriptOutput> {
    @Schema(
            title = "The full service account json key to use to authenticate to gcloud"
    )
    @PluginProperty
    protected String serviceAccount;

    @Schema(
            title = "The project id to scope the commands to"
    )
    @PluginProperty
    protected String projectId;

    @Schema(
            title = "The commands to run"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    protected List<String> commands;

    @Schema(
            title = "Additional environment variables for the current process."
    )
    @PluginProperty(
            additionalProperties = String.class,
            dynamic = true
    )
    protected Map<String, String> env;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = new CommandsWrapper(runContext).withDockerOptions(
                DockerOptions.builder()
                        .image("google/cloud-sdk")
                        .volumes(List.of(System.getProperty("user.home") + "/.config/gcloud:/root/.gcloudConf:ro"))
                        .build()
        ).withRunnerType(RunnerType.DOCKER).withCommands(
                ScriptService.scriptCommands(
                        List.of("/bin/sh", "-c"),
                        List.of("cp -r /root/.gcloudConf/* /root/.config/gcloud/"),
                        this.commands)
        ).withWarningOnStdErr(true);

        commands = commands.withEnv(this.getEnv(runContext));

        return commands.run();
    }

    private Map<String, String> getEnv(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> envs = new HashMap<>();
        if (serviceAccount != null) {
            Path serviceAccountPath = runContext.tempFile(runContext.render(this.serviceAccount).getBytes());
            envs.putAll(Map.of(
                    "GOOGLE_APPLICATION_CREDENTIALS", serviceAccountPath.toString(),
                    "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", serviceAccountPath.toString()
            ));
        }
        if (projectId != null) {
            envs.put("CLOUDSDK_CORE_PROJECT", runContext.render(this.projectId));
        }

        return envs;
    }
}
