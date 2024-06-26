package io.kestra.plugin.gcp.runner;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.batch.v1.*;
import com.google.cloud.batch.v1.Runnable;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.storage.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.*;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.plugin.gcp.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Task runner that executes a task inside a job in Google Cloud Batch.",
    description = """
        This task runner is container-based so the `containerImage` property must be set.
        You need to have roles 'Batch Job Editor' and 'Logs Viewer' to be able to use it.
        
        To access the task's working directory, use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable. Input files and namespace files will be available in this directory.

        To generate output files you can either use the `outputFiles` task's property and create a file with the same name in the task's working directory, or create any file in the output directory which can be accessed by the `{{outputDir}}` Pebble expression or the `OUTPUT_DIR` environment variables.
        
        To use `inputFiles`, `outputFiles` or `namespaceFiles` properties, make sure to set the `bucket` property. The bucket serves as an intermediary storage layer for the task runner. Input and namespace files will be uploaded to the cloud storage bucket before the task run. Similarly, the task runner will store outputFiles in this bucket during the task run. In the end, the task runner will make those files available for download and preview from the UI by sending them to internal storage.
        To make it easier to track where all files are stored, the task runner will generate a folder for each task run. You can access that folder using the `{{bucketPath}}` Pebble expression or the `BUCKET_PATH` environment variable.
        
        Warning, contrarily to other task runners, this task runner didn't run the task in the working directory but in the root directory. You must use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable to access files.
        
        Note that when the Kestra Worker running this task is terminated, the batch job will still runs until completion, then after restarting, the Worker will resume processing on the existing job unless `resume` is set to false."""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command.",
            code = """
                id: new-shell
                namespace: company.team
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.gcp.runner.Batch
                      projectId: "{{vars.projectId}}"
                      region: "{{vars.region}}"
                    commands:
                      - echo "Hello World\"""",
            full = true
        ),
        @Example(
            title = "Pass input files to the task, execute a Shell command, then retrieve output files.",
            code = """
                id: new-shell-with-file
                namespace: company.team
                
                inputs:
                  - id: file
                    type: FILE
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    inputFiles:
                      data.txt: "{{inputs.file}}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.gcp.runner.Batch
                      projectId: "{{vars.projectId}}"
                      region: "{{vars.region}}"
                      bucket: "{{vars.bucker}}"
                    commands:
                      - cp {{workingDir}}/data.txt {{workingDir}}/out.txt""",
            full = true
        )
    }
)
public class Batch extends TaskRunner implements GcpInterface, RemoteRunnerInterface {
    private static final int BUFFER_SIZE = 8 * 1024;
    public static final String MOUNT_PATH = "/mnt/disks/share";

    private String projectId;
    private String serviceAccount;
    @Builder.Default
    private List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/cloud-platform");

    @Schema(
        title = "The GCP region."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String region;

    @Schema(
        title = "The GCP machine type.",
        description = "See https://cloud.google.com/compute/docs/machine-types"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private String machineType = "e2-medium";

    @Schema(
        title = "Compute resource requirements.",
        description = "ComputeResource defines the amount of resources required for each task. Make sure your tasks " +
            "have enough resources to successfully run. If you also define the types of resources for a job to use with the " +
            "[InstancePolicyOrTemplate](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#instancepolicyortemplate) " +
            "field, make sure both fields are compatible with each other."
    )
    @PluginProperty
    private ComputeResource computeResource;

    @Schema(
        title = "Compute reservation."
    )
    @PluginProperty(dynamic = true)
    private String reservation;

    @Schema(
        title = "Container entrypoint to use."
    )
    @PluginProperty(dynamic = true)
    private List<String> entryPoint;

    @Schema(
        title = "Network interfaces."
    )
    @PluginProperty
    private List<NetworkInterface> networkInterfaces;

    @Schema(
        title = "Google Cloud Storage Bucket to use to upload (`inputFiles` and `namespaceFiles`) and download (`outputFiles`) files.",
        description = "It's mandatory to provide a bucket if you want to use such properties."
    )
    @PluginProperty(dynamic = true)
    private String bucket;

    @Schema(
        title = "The maximum duration to wait for the job completion unless the task `timeout` property is set which will take precedence over this property.",
        description = "Google Cloud Batch will automatically timeout the job upon reaching such duration and the task will be failed."
    )
    @Builder.Default
    @PluginProperty
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Whether the job should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Boolean delete = true;

    @Schema(
        title = "Whether to reconnect to the current job if it already exists."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Boolean resume = true;

    @Schema(
        title = "Determines how often Kestra should poll the container for completion. By default, the task runner checks every 5 seconds whether the job is completed. You can set this to a lower value (e.g. `PT0.1S` = every 100 milliseconds) for quick jobs and to a lower threshold (e.g. `PT1M` = every minute) for long-running jobs. Setting this property to a lower value will reduce the number of API calls Kestra makes to the remote service — keep that in mind in case you see API rate limit errors."
    )
    @Builder.Default
    @PluginProperty
    private final Duration completionCheckInterval = Duration.ofSeconds(5);

    @Schema(
        title = "Additional time after the job ends to wait for late logs."
    )
    @Builder.Default
    @PluginProperty
    private final Duration waitForLogInterval = Duration.ofSeconds(5);

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        String renderedBucket = runContext.render(this.bucket);

        final GoogleCredentials credentials = CredentialService.credentials(runContext, this);

        boolean hasFilesToUpload = !ListUtils.isEmpty(filesToUpload);
        if (hasFilesToUpload && bucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
        if ((hasFilesToDownload || outputDirectoryEnabled) && bucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `outputFiles` or `{{ outputDir }}`");
        }

        Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);
        Path batchWorkingDirectory = (Path) additionalVars.get(ScriptService.VAR_WORKING_DIR);
        String workingDirectoryToBlobPath = batchWorkingDirectory.toString().substring(1);
        boolean hasBucket = this.bucket != null;

        String projectId = runContext.render(this.projectId);

        LoggingOptions.Builder loggingOptionsBuilder = LoggingOptions.newBuilder().setCredentials(credentials);
        if (projectId != null) {
            loggingOptionsBuilder.setProjectId(projectId);
        }

        try (BatchServiceClient batchServiceClient = newBatchServiceClient(credentials);
             Logging logging = loggingOptionsBuilder.build().getService()) {
            Duration waitDuration = Optional.ofNullable(taskCommands.getTimeout()).orElse(this.waitUntilCompletion);
            Map<String, String> labels = LabelUtils.labels(runContext);

            Job result = null;

            String region = runContext.render(this.region);

            if (resume) {
                var existingJob = batchServiceClient.listJobs(ListJobsRequest.newBuilder()
                    .setParent(String.format("projects/%s/locations/%s", projectId, region))
                    .setFilter(labelsFilter(labels))
                    .build()
                );
                var iterator = existingJob.iterateAll().iterator();
                if (iterator.hasNext()) {
                    result = iterator.next();
                    runContext.logger().info("Job '{}' is resumed from an already running job ", result.getName());
                }
            }
            Path outputDirectory = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
            if (result == null) {
                if (hasFilesToUpload || outputDirectoryEnabled) {
                    GcsUtils.of(projectId, credentials).uploadFiles(runContext,
                        filesToUpload,
                        renderedBucket,
                        batchWorkingDirectory,
                        outputDirectory,
                        outputDirectoryEnabled
                    );
                }

                var taskBuilder = TaskSpec.newBuilder();
                taskBuilder.setMaxRunDuration(com.google.protobuf.Duration.newBuilder().setSeconds(waitDuration.getSeconds()));

                if (this.computeResource != null) {
                    com.google.cloud.batch.v1.ComputeResource.Builder computeResourceBuilder = com.google.cloud.batch.v1.ComputeResource.newBuilder();

                    if (this.computeResource.bootDisk != null) {
                        computeResourceBuilder.setBootDiskMib(this.computeResource.bootDisk);
                    }

                    if (this.computeResource.cpu != null) {
                        computeResourceBuilder.setCpuMilli(this.computeResource.cpu);
                    }

                    if (this.computeResource.memory != null) {
                        computeResourceBuilder.setMemoryMib(this.computeResource.memory);
                    }

                    taskBuilder.setComputeResource(computeResourceBuilder.build());
                }

                if (hasFilesToDownload || hasFilesToUpload || outputDirectoryEnabled) {
                    taskBuilder.addVolumes(Volume.newBuilder()
                        .setGcs(GCS.newBuilder().setRemotePath(renderedBucket + batchWorkingDirectory).build())
                        .setMountPath(MOUNT_PATH)
                        .build()
                    );
                }

                // main container
                Runnable runnable =
                    Runnable.newBuilder()
                        .setContainer(mainContainer(runContext, taskCommands, taskCommands.getCommands(), hasFilesToDownload || hasFilesToUpload || outputDirectoryEnabled, batchWorkingDirectory))
                        .setEnvironment(Environment.newBuilder()
                            .putAllVariables(this.env(runContext, taskCommands))
                            .build()
                        )
                        .build();
                taskBuilder.addRunnables(runnable);

                TaskGroup taskGroup = TaskGroup.newBuilder().setTaskSpec(taskBuilder.build()).setTaskCount(1).build();

                // https://cloud.google.com/compute/docs/machine-types
                var instancePolicy = AllocationPolicy.InstancePolicy.newBuilder().setMachineType(runContext.render(machineType));
                if (reservation != null) {
                    instancePolicy.setReservation(runContext.render(reservation));
                }
                var allocationPolicy = AllocationPolicy.newBuilder()
                    .addInstances(AllocationPolicy.InstancePolicyOrTemplate.newBuilder().setPolicy(instancePolicy).build());
                if (!ListUtils.isEmpty(networkInterfaces)) {
                    var networkPolicy = AllocationPolicy.NetworkPolicy.newBuilder();
                    networkInterfaces.forEach(throwConsumer(networkInterface -> {
                        var builder = AllocationPolicy.NetworkInterface.newBuilder()
                            .setNetwork(runContext.render(networkInterface.getNetwork()));
                        if (networkInterface.getSubnetwork() != null) {
                            builder.setSubnetwork(runContext.render(networkInterface.getSubnetwork()));
                        }
                        networkPolicy.addNetworkInterfaces(builder);
                    }));
                    allocationPolicy.setNetwork(networkPolicy.build());
                }

                Job job =
                    Job.newBuilder()
                        .addTaskGroups(taskGroup)
                        .setAllocationPolicy(allocationPolicy)
                        .putAllLabels(labels)
                        // We use Cloud Logging as it's an out of the box available option.
                        .setLogsPolicy(LogsPolicy.newBuilder().setDestination(LogsPolicy.Destination.CLOUD_LOGGING).build())
                        .build();

                CreateJobRequest createJobRequest =
                    CreateJobRequest.newBuilder()
                        // The job's parent is the region in which the job will run.
                        .setParent(String.format("projects/%s/locations/%s", projectId, region))
                        .setJob(job)
                        .setJobId(ScriptService.jobName(runContext))
                        .build();

                result = batchServiceClient.createJob(createJobRequest);

                final String jobName = result.getName();
                runContext.logger().info("Job created: {}", jobName);
            }
            Job finalResult = result;
            onKill(() -> safelyKillJob(runContext, credentials, finalResult.getName()));


            // Check for the job successful creation
            if (isFailed(result.getStatus().getState())) {
                throw new TaskException(result.getStatus().getState().getNumber(), taskCommands.getLogConsumer().getStdOutCount(), taskCommands.getLogConsumer().getStdErrCount());
            }

            // if needed, batch infrastructure logs can be retrieved by using logName="projects/%s/logs/batch_task_logs" OR "%s/logs/batch_agent_logs"
            String logFilter = String.format(
                "logName=\"projects/%s/logs/batch_task_logs\" labels.job_uid=\"%s\"",
                projectId,
                result.getUid()
            );
            LogEntryServerStream stream = logging.tailLogEntries(Logging.TailOption.filter(logFilter));
            try (LogTail ignored = new LogTail(stream, taskCommands.getLogConsumer(), this.waitForLogInterval)) {
                runContext.logger().info("Waiting for job completion.");
                // Wait for the job termination
                result = waitForTerminated(batchServiceClient, result, waitDuration);
                if (result == null) {
                    throw new TimeoutException();
                }
                if (isFailed(result.getStatus().getState())) {
                    throw new TaskException(result.getStatus().getState().getNumber(), taskCommands.getLogConsumer().getStdOutCount(), taskCommands.getLogConsumer().getStdErrCount());
                }

                if (delete) {
                    batchServiceClient.deleteJobAsync(result.getName());
                    runContext.logger().info("Job deleted");
                }

                if (hasFilesToDownload || outputDirectoryEnabled) {
                    GcsUtils.of(projectId, credentials).downloadFile(
                        runContext,
                        taskCommands,
                        filesToDownload,
                        renderedBucket,
                        batchWorkingDirectory,
                        outputDirectory,
                        outputDirectoryEnabled
                    );
                }

                return new RunnerResult(0, taskCommands.getLogConsumer());
            }
        } finally {
            if (hasBucket && delete) {
                try (Storage storage = GcsUtils.of(projectId, credentials).storage(runContext)) {
                    Page<Blob> list = storage.list(renderedBucket, Storage.BlobListOption.prefix(workingDirectoryToBlobPath));
                    list.iterateAll().forEach(blob -> storage.delete(blob.getBlobId()));
                    storage.delete(BlobInfo.newBuilder(BlobId.of(renderedBucket, workingDirectoryToBlobPath)).build().getBlobId());
                }
            }
        }
    }

    private static BatchServiceClient newBatchServiceClient(GoogleCredentials credentials) throws IOException {
        return BatchServiceClient.create(BatchServiceSettings.newBuilder().setCredentialsProvider(() -> credentials).build());
    }

    private String labelsFilter(Map<String, String> labels) {
        return labels.entrySet().stream()
            .map(entry -> "labels." + entry.getKey() + "=\"" + entry.getValue().toLowerCase() + "\"")
            .collect(Collectors.joining(" AND "));
    }

    private Runnable.Container mainContainer(RunContext runContext, TaskCommands taskCommands, List<String> command, boolean mountVolume, Path batchWorkingDirectory) throws IllegalVariableEvaluationException {
        // TODO working directory
        var builder = Runnable.Container.newBuilder()
            .setImageUri(runContext.render(taskCommands.getContainerImage()))
            .addAllCommands(command);

        if (mountVolume) {
            builder.addVolumes(MOUNT_PATH + ":" + batchWorkingDirectory.toString());
        }

        if (this.entryPoint != null) {
            builder.setEntrypoint(String.join(" ", runContext.render(this.entryPoint)));
        }

        return builder.build();
    }

    private Job waitForTerminated(BatchServiceClient batchServiceClient, Job result, Duration waitDuration) throws TimeoutException {
        return Await.until(
            () -> {
                Job terminated = batchServiceClient.getJob(result.getName());
                if (isTerminated(terminated.getStatus().getState())) {
                    return terminated;
                }
                return null;
            },
            completionCheckInterval,
            waitDuration
        );
    }

    private boolean isFailed(JobStatus.State state) {
        return state == JobStatus.State.STATE_UNSPECIFIED || state == JobStatus.State.FAILED || state == JobStatus.State.UNRECOGNIZED;
    }

    private boolean isTerminated(JobStatus.State state) {
        return state == JobStatus.State.SUCCEEDED || state == JobStatus.State.DELETION_IN_PROGRESS || isFailed(state);
    }

    @Override
    protected Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) throws IllegalVariableEvaluationException {
        Map<String, Object> additionalVars = new HashMap<>();
        Path batchWorkingDirectory = Path.of("/" + IdUtils.create());
        additionalVars.put(ScriptService.VAR_WORKING_DIR, batchWorkingDirectory);

        if (bucket != null) {
            Path batchOutputDirectory = batchWorkingDirectory.resolve(IdUtils.create());
            additionalVars.put(ScriptService.VAR_BUCKET_PATH, "gs://" + runContext.render(this.bucket) + batchWorkingDirectory);

            if (taskCommands.outputDirectoryEnabled()) {
                additionalVars.put(ScriptService.VAR_OUTPUT_DIR, batchOutputDirectory);
            }
        }

        return additionalVars;
    }

    private void safelyKillJob(final RunContext runContext,
                               final GoogleCredentials credentials,
                               final String jobName) {
        // Use a dedicated BatchServiceClient, as the one used in the run method may be closed in the meantime.
        try (BatchServiceClient batchServiceClient = newBatchServiceClient(credentials)) {
            final Job job = batchServiceClient.getJob(jobName);
            if (isTerminated(job.getStatus().getState())) {
                // Job execution is already terminated, so we can skip deletion.
                return;
            }

            final DeleteJobRequest request = DeleteJobRequest.newBuilder()
                .setName(jobName)
                .setReason("Kestra task was killed.")
                .build();

            batchServiceClient.deleteJobAsync(request).get();
            runContext.logger().debug("Job deleted: {}", jobName);
            // we don't need to clean up the storage here as this will be
            // properly handle by the Task Thread in the run method once the job is terminated (i.e., deleted).
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | IOException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            runContext.logger().warn("Failed to delete Job: {}", jobName, t);
        }
    }

    @Getter
    @Builder
    public static class NetworkInterface {
        @Schema(title = "Network identifier with the format `projects/HOST_PROJECT_ID/global/networks/NETWORK`.")
        @PluginProperty(dynamic = true)
        @NotNull
        private String network;

        @Schema(title = "Subnetwork identifier in the format `projects/HOST_PROJECT_ID/regions/REGION/subnetworks/SUBNET`")
        @PluginProperty(dynamic = true)
        private String subnetwork;
    }

    @Getter
    @Builder
    public static class ComputeResource {
        @Schema(
            title = "The milliCPU count.",
            description = "Defines the amount of CPU resources per task in milliCPU units. For example, `1000` " +
                "corresponds to 1 vCPU per task. If undefined, the default value is `2000`." +
                "\n" +
                "If you also define the VM's machine type using the `machineType` property in " +
                "[InstancePolicy](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#instancepolicy) " +
                "field or inside the `instanceTemplate` in the " +
                "[InstancePolicyOrTemplate](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#instancepolicyortemplate) " +
                "field, make sure the CPU resources for both fields are compatible with each other and with how many " +
                "tasks you want to allow to run on the same VM at the same time.\n" +
                "\n" +
                "For example, if you specify the `n2-standard-2` machine type, which has 2 vCPUs, you can " +
                "set the `cpu` to no more than `2000`. Alternatively, you can run two tasks on the same VM " +
                "if you set the `cpu` to `1000` or less."
        )
        @PluginProperty
        private Integer cpu;

        @Schema(
            title = "Memory in MiB.",
            description = "Defines the amount of memory per task in MiB units. If undefined, the default value is `2000`. " +
                "If you also define the VM's machine type using the `machineType` in " +
                "[InstancePolicy](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#instancepolicy) " +
                "field or inside the `instanceTemplate` in the " +
                "[InstancePolicyOrTemplate](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#instancepolicyortemplate) " +
                "field, make sure the memory resources for both fields are compatible with each other and with how many " +
                "tasks you want to allow to run on the same VM at the same time.\n" +
                "\n" +
                "For example, if you specify the `n2-standard-2` machine type, which has 8 GiB of memory, you can set " +
                "the `memory` to no more than `8192`. Alternatively, you can run two tasks on the same VM " +
                "if you set the `memory` to `4096` or less."
        )
        @PluginProperty
        private Integer memory;

        @Schema(title = "Extra boot disk size in MiB for each task.")
        @PluginProperty
        private Integer bootDisk;
    }
}
