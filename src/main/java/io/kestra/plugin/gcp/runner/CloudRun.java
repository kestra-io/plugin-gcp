package io.kestra.plugin.gcp.runner;

import com.google.api.LaunchStage;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.run.v2.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.RemoteRunnerInterface;
import io.kestra.core.models.tasks.runners.RunnerResult;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.models.tasks.runners.TaskException;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ListUtils;
import io.kestra.core.utils.MapUtils;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.plugin.gcp.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Task runner that executes a task inside a job in Google Cloud Run.",
    description = """
        This task runner is container-based so the `containerImage` property must be set.
        You need to have roles 'Cloud Run Developer' and 'Logs Viewer' to be able to use it.

        To access the task's working directory, use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable. Input files and namespace files will be available in this directory.

        To generate output files you can either use the `outputFiles` task's property and create a file with the same name in the task's working directory, or create any file in the output directory which can be accessed by the `{{outputDir}}` Pebble expression or the `OUTPUT_DIR` environment variables.

        To use `inputFiles`, `outputFiles` or `namespaceFiles` properties, make sure to set the `bucket` property. The bucket serves as an intermediary storage layer for the task runner. Input and namespace files will be uploaded to the cloud storage bucket before the task run. Similarly, the task runner will store outputFiles in this bucket during the task run. In the end, the task runner will make those files available for download and preview from the UI by sending them to internal storage.
        To make it easier to track where all files are stored, the task runner will generate a folder for each task run. You can access that folder using the `{{bucketPath}}` Pebble expression or the `BUCKET_PATH` environment variable.

        Warning, contrarily to other task runners, this task runner didn't run the task in the working directory but in the root directory. You must use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable to access files.

        Note that when the Kestra Worker running this task is terminated, the Cloud Run Job will still run until completion."""
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
                      type: io.kestra.plugin.gcp.runner.CloudRun
                      projectId: "{{ vars.projectId }}"
                      region: "{{ vars.region }}"
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
                      data.txt: "{{ inputs.file }}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.gcp.runner.CloudRun
                      projectId: "{{ vars.projectId }}"
                      region: "{{ vars.region }}"
                      bucket: "{{ vars.bucker }}"
                    commands:
                      - cp {{ workingDir }}/data.txt {{ workingDir }}/out.txt""",
            full = true
        )
    }
)
public class CloudRun extends TaskRunner implements GcpInterface, RemoteRunnerInterface {
    public static final Path MOUNT_PATH = Path.of("/mnt/disks/share");
    public static final String VOLUME_NAME = "kestra-io";

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
        title = "Google Cloud Storage Bucket to use to upload (`inputFiles` and `namespaceFiles`) and download (`outputFiles`) files.",
        description = "It's mandatory to provide a bucket if you want to use such properties."
    )
    @PluginProperty(dynamic = true)
    private String bucket;

    @Schema(
        title = "The maximum duration to wait for the job completion unless the task `timeout` property is set which will take precedence over this property.",
        description = "Google Cloud Run will automatically timeout the Job upon reaching such duration and the task will be failed."
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
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {

        final String renderedProjectId = runContext.render(projectId);
        final String renderedBucket = runContext.render(bucket);
        final String renderedRegion = runContext.render(region);
        final GoogleCredentials credentials = CredentialService.credentials(runContext, this);

        Logger logger = runContext.logger();
        List<Path> relativeWorkingDirectoryFilesPaths = taskCommands.relativeWorkingDirectoryFilesPaths();
        boolean hasFilesToUpload = !ListUtils.isEmpty(relativeWorkingDirectoryFilesPaths);
        if (hasFilesToUpload && bucket == null) {
            logger.warn("Working directory is not empty but no Cloud Storage bucket are specified. You must provide a Cloud Storage bucket in order to use `inputFiles` or `namespaceFiles`. Skipping importing files to runner.");
        }
        final boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        final boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
        final boolean hasVolume = hasFilesToDownload || hasFilesToUpload || outputDirectoryEnabled;

        if ((hasFilesToDownload || outputDirectoryEnabled) && bucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `outputFiles` or `{{ outputDir }}`");
        }

        Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);

        final Path workingDir = (Path) additionalVars.get(ScriptService.VAR_WORKING_DIR);
        final Path outputDir = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);

        final String workingDirectoryToBlobPath = MOUNT_PATH.relativize(workingDir).toString();

        LoggingOptions.Builder loggingOptionsBuilder = LoggingOptions.newBuilder().setCredentials(credentials);
        if (renderedProjectId != null) {
            loggingOptionsBuilder.setProjectId(renderedProjectId);
        }
        try (JobsClient jobsClient = newJobsClient(credentials);
             ExecutionsClient executionsClient = newExecutionsClient(credentials);
             Logging logging = loggingOptionsBuilder.build().getService()) {

            Duration timeout = getTaskTimeout(taskCommands);
            Job job = null;
            Execution execution = null;
            final String jobName = ScriptService.jobName(runContext);

            if (resume) {
                var listJobsPagedResponse = jobsClient.listJobs(LocationName.of(renderedProjectId, renderedRegion).toString());
                Optional<Job> existingJob = StreamSupport.stream(listJobsPagedResponse.iterateAll().spliterator(), false)
                    .filter(candidate -> hasAllLabels(candidate.getLabelsMap(), LabelUtils.labels(runContext)))
                    .findFirst();
                if (existingJob.isPresent()) {
                    job = existingJob.get();
                    Optional<Execution> existingExecution = StreamSupport.stream(executionsClient.listExecutions(job.getName()).iterateAll().spliterator(), false).findFirst();
                    if (existingExecution.isPresent()) {
                        execution = existingExecution.get();
                        logger.info("Job '{}' is resumed from an already running job execution ({})", job.getName(), execution.getName());
                    }
                }

            }

            if (execution == null) {
                if (hasFilesToUpload || outputDirectoryEnabled) {
                    GcsUtils.of(renderedProjectId, credentials).uploadFiles(runContext,
                        relativeWorkingDirectoryFilesPaths,
                        renderedBucket,
                        toAbsoluteBlobPathWithoutMount(workingDir),
                        toAbsoluteBlobPathWithoutMount(outputDir),
                        outputDirectoryEnabled
                    );
                } else if (hasFilesToDownload) {
                    // Create a blob for the WORKING_DIR to be able to mount it as a directory in the container volume.
                    // Used to prevent 'chdir' to fail with no such file exists.
                    BlobInfo workingDirBlob = BlobInfo.newBuilder(BlobId.of(renderedBucket, workingDirectoryToBlobPath + "/")).build();
                    try (Storage storage = GcsUtils.of(renderedProjectId, credentials).storage(runContext)) {
                        storage.create(workingDirBlob);
                    }
                }

                // Create new Job TaskTemplate
                TaskTemplate.Builder taskBuilder = TaskTemplate.newBuilder();

                if (hasVolume) {
                    // Create and add Volume
                    Volume volume = Volume.newBuilder()
                        .setGcs(
                            GCSVolumeSource
                                .newBuilder()
                                .setReadOnly(false)
                                .setBucket(renderedBucket)
                                .build()
                        )
                        .setName(VOLUME_NAME)
                        .build();
                    taskBuilder.addVolumes(volume);
                }

                // Create and add Container
                Container.Builder containerBuilder = Container.newBuilder()
                    .setImage(taskCommands.getContainerImage())
                    .addAllCommand(taskCommands.getCommands())
                    .addAllEnv(envVars(runContext, taskCommands));

                if (hasVolume) {
                    // Mount the volume
                    containerBuilder.setWorkingDir(workingDir.toString());
                    containerBuilder.addVolumeMounts(
                        VolumeMount.newBuilder()
                            .setName(VOLUME_NAME)
                            .setMountPath(MOUNT_PATH.toString())
                            .build()
                    );
                }

                taskBuilder.addContainers(containerBuilder);
                TaskTemplate task = taskBuilder
                    .build();

                // Create Job Definition
                job = Job.newBuilder()
                    .setLaunchStage(LaunchStage.BETA) // required: Volume is a BETA feature for CloudRun Job.
                    .setTemplate(ExecutionTemplate
                        .newBuilder()
                        .setTemplate(task)
                        .setTaskCount(1)
                        .build()
                    )
                    .putAllLabels(LabelUtils.labels(runContext))
                    .build();

                final CreateJobRequest createJobRequest =
                    CreateJobRequest.newBuilder()
                        // The job's parent is the region in which the job will run.
                        .setParent(LocationName.of(renderedProjectId, renderedRegion).toString())
                        .setJob(job)
                        .setJobId(jobName)
                        .build();

                Job jobCreated = jobsClient.createJobAsync(createJobRequest).get();
                logger.info("Job created: {}", jobCreated.getName());

                // Run the Job

                final JobName fullJobName = JobName.of(renderedProjectId, renderedRegion, jobName);
                final RunJobRequest runJobRequest =
                    RunJobRequest.newBuilder()
                        .setName(fullJobName.toString())
                        .setOverrides(RunJobRequest.Overrides
                            .newBuilder()
                            .setTaskCount(1)
                            .setTimeout(com.google.protobuf.Duration
                                .newBuilder()
                                .setSeconds(timeout.getSeconds())
                                .build()
                            )
                            .build()
                        )
                        .build();

                OperationFuture<Execution, Execution> future = jobsClient.runJobAsync(runJobRequest);
                execution = future.getMetadata().get();
            }
            
            final String executionName = execution.getName();
            
            onKill(() -> safelyKillJob(runContext, credentials, executionName));

            LogEntryServerStream stream = logging.tailLogEntries(
                Logging.TailOption.filter("(logName=projects/" + renderedProjectId + "/logs/run.googleapis.com%2Fstdout OR logName=projects/" + renderedProjectId + "/logs/run.googleapis.com%2Fstderr) AND " +
                    "resource.labels.job_name=\"" + jobName + "\"")
            );
            try (LogTail ignored = new LogTail(stream, taskCommands.getLogConsumer(), this.waitForLogInterval)) {
                if (!isTerminated(execution)) {
                    logger.info("Waiting for execution completion: {}.", executionName);
                    execution = awaitJobExecutionTermination(executionsClient, executionName, timeout);
                }
                // Check for the job successful creation
                if (isFailed(execution)) {
                    throw new TaskException(
                        -1,
                        taskCommands.getLogConsumer().getStdOutCount(),
                        taskCommands.getLogConsumer().getStdErrCount()
                    );
                }

                if (delete) {
                    // not waiting for Job Execution deletion
                    executionsClient.deleteExecutionAsync(executionName);
                    logger.info("Job Execution deleted: {}", executionName);
                    // not waiting for Job deletion
                    jobsClient.deleteJobAsync(jobName);
                    logger.info("Job deleted: {}", jobName);
                }

                if (hasFilesToDownload || outputDirectoryEnabled) {
                    GcsUtils.of(renderedProjectId, credentials).downloadFile(
                        runContext,
                        taskCommands,
                        filesToDownload,
                        renderedBucket,
                        MOUNT_PATH.relativize(workingDir),
                        outputDir != null ? MOUNT_PATH.relativize(outputDir) : null,
                        outputDirectoryEnabled
                    );
                }
                return new RunnerResult(0, taskCommands.getLogConsumer());
            }
        } finally {
            if (bucket != null && delete) {
                GcsUtils.of(renderedProjectId, credentials).deleteBucket(runContext, bucket, workingDirectoryToBlobPath);
            }
        }
    }

    private boolean hasAllLabels(Map<String, String> labelsMap, Map<String, String> labels) {
        return labelsMap.entrySet().containsAll(labels.entrySet());
    }

    private static ExecutionsClient newExecutionsClient(GoogleCredentials credentials) throws IOException {
        return ExecutionsClient.create(ExecutionsSettings.newBuilder().setCredentialsProvider(() -> credentials).build());
    }
    
    private static JobsClient newJobsClient(GoogleCredentials credentials) throws IOException {
        return JobsClient.create(JobsSettings.newBuilder().setCredentialsProvider(() -> credentials).build());
    }
    
    private Duration getTaskTimeout(final TaskCommands taskCommands) {
        return Optional
            .ofNullable(taskCommands.getTimeout())
            .filter(timout -> !timout.equals(Duration.ZERO))
            .orElse(this.waitUntilCompletion);
    }

    private List<EnvVar> envVars(RunContext runContext, TaskCommands taskCommands) throws IllegalVariableEvaluationException {
        return this.env(runContext, taskCommands).entrySet()
            .stream()
            .map(it -> EnvVar
                .newBuilder()
                .setName(it.getKey())
                .setValue(it.getValue())
                .build()
            )
            .toList();
    }

    private Execution awaitJobExecutionTermination(final ExecutionsClient executionsClient,
                                                   final String executionName,
                                                   final Duration timeout) throws TimeoutException {
        return Await.until(
            () -> {
                Execution execution = executionsClient.getExecution(executionName);
                if (isTerminated(execution)) {
                    return execution;
                }
                return null;
            },
            completionCheckInterval,
            timeout
        );
    }

    private static Path toAbsoluteBlobPathWithoutMount(final Path containerPath) {
        return containerPath != null ? Path.of("/" + MOUNT_PATH.relativize(containerPath)) : null;
    }

    private static boolean isCanceled(final Execution execution) {
        return execution.getRunningCount() == 0 && execution.getCancelledCount() == 1;
    }

    private static boolean isFailed(final Execution execution) {
        return execution.getRunningCount() == 0 && execution.getFailedCount() == 1;
    }

    private static boolean isSucceeded(final Execution execution) {
        return execution.getRunningCount() == 0 && execution.getSucceededCount() == 1;
    }

    private static boolean isTerminated(final Execution execution) {
        return isSucceeded(execution) || isCanceled(execution) || isFailed(execution);
    }

    @Override
    protected Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) throws IllegalVariableEvaluationException {
        Map<String, Object> additionalVars = new HashMap<>();
        String workingDirId = IdUtils.create();
        additionalVars.put(ScriptService.VAR_WORKING_DIR, MOUNT_PATH.resolve(workingDirId));

        if (bucket != null) {
            String outputDirId = IdUtils.create();
            Path outputDir = Path.of("/" + workingDirId).resolve(outputDirId);
            additionalVars.put(ScriptService.VAR_BUCKET_PATH, "gs://" + runContext.render(this.bucket) + outputDir);

            if (taskCommands.outputDirectoryEnabled()) {
                additionalVars.put(ScriptService.VAR_OUTPUT_DIR, MOUNT_PATH.resolve(workingDirId).resolve(outputDirId));
            }
        }

        return additionalVars;
    }
    
    private void safelyKillJob(final RunContext runContext,
                               final GoogleCredentials credentials,
                               final String executionName) {
        // Use a dedicated ExecutionsClient, as the one used in the run method may be closed in the meantime.
        try (ExecutionsClient executionsClient = newExecutionsClient(credentials)) {
            Execution execution = executionsClient.getExecution(executionName);
            if (isTerminated(execution)) {
                // Execution is already terminated so we can skip deletion.
                return;
            }
            executionsClient.cancelExecutionAsync(executionName).get();
            runContext.logger().debug("Job execution canceled: {}", executionName);
            // we don't need to clean up the storage and execution here as this will be
            // properly handle by the Task Thread in the run method once the job is terminated (i.e., deleted).
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | IOException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            runContext.logger().warn("Failed to cancel Job execution: {}", executionName, t);
        }
    }
}
