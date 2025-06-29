package io.kestra.plugin.gcp.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
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
                        full = true,
                        title = "Create a cluster then list them using a service account.",
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
                        title = "Create a GCS bucket.",
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
                        title = "Output the result of a command.",
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
                                  - gcloud pubsub topics list --format=json | tr -d '\n ' | xargs -0 -I {} echo '::{"outputs":{"gcloud":{}}}::'

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
                              - gcloud storage ls --json | tr -d '\n ' | xargs -0 -I {} echo
                                '::{"outputs":{"gcloud":{}}}::'
                        """
                )
        }
)
public class GCloudCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "google/cloud-sdk";

    @Schema(
        title = "The full service account JSON key to use to authenticate to gcloud."
    )
    protected Property<String> serviceAccount;

    @Schema(
        title = "The GCP project ID to scope the commands to."
    )
    protected Property<String> projectId;

    @Schema(
        title = "The commands to run."
    )
    @NotNull
    protected Property<List<String>> commands;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {

        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withDockerOptions(injectDefaults(getDocker()))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.containerImage).as(String.class).orElseThrow())
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(this.commands)
            .withEnv(this.getEnv(runContext))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles);

        return commands.run();
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    private Map<String, String> getEnv(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> envs = new HashMap<>();

        if (serviceAccount != null) {
            Path serviceAccountPath = runContext.workingDir().createTempFile(runContext.render(this.serviceAccount).as(String.class).orElseThrow().getBytes());
            envs.putAll(Map.of(
                    "GOOGLE_APPLICATION_CREDENTIALS", serviceAccountPath.toString(),
                    "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", serviceAccountPath.toString()
            ));
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
