package io.kestra.plugin.gcp.cli;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.TargetOS;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
// callSuper = false: the inherited env has no @ToString.Exclude, keeps rendered secrets out
@ToString(callSuper = false)
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run gcloud commands in a task runner",
    description = """
        Executes one or more `gcloud` commands via the configured task runner (Docker by default with image `google/cloud-sdk`). Supports injecting a service account key and project ID into env vars and collecting output files.
        The inherited `warningOnStdErr` and `runner` properties are ignored. The deprecated `docker` option only applies on Kestra 1.3, use `taskRunner`.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Create a cluster then list them using a service account",
            code = """
                id: gcp_g_cloud_cli
                namespace: company.team

                tasks:
                  - id: g_cloud_cli
                    type: io.kestra.plugin.gcp.cli.GCloudCLI
                    projectId: my-gcp-project
                    serviceAccount: "{{ secret('gcp-sa') }}"
                    commands:
                      - gcloud container clusters create simple-cluster --region=europe-west3
                      - gcloud container clusters list
                """
        ),
        @Example(
            full = true,
            title = "Create a GCS bucket",
            code = """
                id: gcp_g_cloud_cli
                namespace: company.team

                tasks:
                  - id: g_cloud_cli
                    type: io.kestra.plugin.gcp.cli.GCloudCLI
                    projectId: my-gcp-project
                    serviceAccount: "{{ secret('gcp-sa') }}"
                    commands:
                      - gcloud storage buckets create gs://my-bucket
                """
        ),
        @Example(
            full = true,
            title = "Output the result of a command",
            code = """
                id: gcp_g_cloud_cli
                namespace: company.team

                tasks:
                  - id: g_cloud_cli
                    type: io.kestra.plugin.gcp.cli.GCloudCLI
                    projectId: my-gcp-project
                    serviceAccount: "{{ secret('gcp-sa') }}"
                    commands:
                      # Outputs as a flow output for UI display
                      - gcloud pubsub topics list --format=json | tr -d '\\n ' | xargs -0 -I {} echo '::{"outputs":{"gcloud":{}}}::'

                      # Outputs as a file, preferred way for large payloads
                      - gcloud storage ls --json > storage.json
                """
        ),
        @Example(
            full = true,
            title = "List storage buckets in a given GCP project and output the result",
            code = """
                id: gcloud_cli_flow
                namespace: company.team

                tasks:
                  - id: gcloud_cli
                    type: io.kestra.plugin.gcp.cli.GCloudCLI
                    serviceAccount: "{{ secret('GCP_CREDS') }}"
                    projectId: yourProject
                    outputFiles:
                      - storage.json
                    commands:
                      - gcloud storage ls
                      - gcloud storage ls --json > storage.json
                      - gcloud storage ls --json | tr -d '\\n ' | xargs -0 -I {} echo
                        '::{"outputs":{"gcloud":{}}}::'
                """
        )
    }
)
public class GCloudCLI extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "google/cloud-sdk";

    @Schema(
        title = "Service account key",
        description = "Full JSON key content written to a temp file and wired to `GOOGLE_APPLICATION_CREDENTIALS` and `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`"
    )
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> serviceAccount;

    @Schema(
        title = "Project ID",
        description = "Optional default project exported as CLOUDSDK_CORE_PROJECT"
    )
    @PluginProperty(group = "connection")
    protected Property<String> projectId;

    @Schema(
        title = "Commands",
        description = "Shell commands executed through the `interpreter` (`/bin/sh -c` by default); rendered before execution"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<List<String>> commands;

    // Kept here: core has no docker property from 2.0, flows using it would fail validation
    @Schema(
        title = "Docker options (deprecated)",
        description = "Deprecated; use `taskRunner` instead"
    )
    @PluginProperty(group = "deprecated")
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner plugin configuration; defaults to Docker"
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Container image",
        description = "Used when the task runner is container-based; defaults to google/cloud-sdk"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Fail fast on the first failing command",
        description = """
            If true, `set -e` is prepended to `beforeCommands`, so the first failing line stops the run.
            Defaults to false, so a failing line does not stop the run.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> failFast = Property.ofValue(false);

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var commands = this.commands(runContext)
            .withInterpreter(this.getInterpreter())
            .withBeforeCommands(this.getBeforeCommands())
            .withBeforeCommandsWithOptions(true)
            .withCommands(this.commands);

        // getEnv returns the credentials then the user env, replaces the map commands() set (immutable for Property.ofValue)
        return commands
            .withEnv(this.getEnv(runContext, commands))
            .run();
    }

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    private Map<String, String> getEnv(RunContext runContext, CommandsWrapper commands) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> envs = new HashMap<>();

        if (serviceAccount != null) {
            Path serviceAccountPath = runContext.workingDir().createTempFile(runContext.render(this.serviceAccount).as(String.class).orElseThrow().getBytes());

            // Remap the worker-absolute temp path to the runner working dir (e.g. /kestra/working-dir on
            // Kubernetes) so gcloud finds the key inside the container; on Docker it resolves unchanged.
            String relativePath = runContext.workingDir().path().relativize(serviceAccountPath).toString();
            String credentialPath = commands.getTaskRunner().toAbsolutePath(runContext, commands, relativePath, TargetOS.LINUX);

            envs.putAll(
                Map.of(
                    "GOOGLE_APPLICATION_CREDENTIALS", credentialPath,
                    "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", credentialPath
                )
            );
        }

        if (projectId != null) {
            envs.put("CLOUDSDK_CORE_PROJECT", runContext.render(this.projectId).as(String.class).orElseThrow());
        }

        var renderedEnv = runContext.render(this.env).asMap(String.class, String.class);
        if (!renderedEnv.isEmpty()) {
            envs.putAll(renderedEnv);
        }

        return envs;
    }
}
