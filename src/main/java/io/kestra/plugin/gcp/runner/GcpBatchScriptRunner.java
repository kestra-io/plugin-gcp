package io.kestra.plugin.gcp.runner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.batch.v1.*;
import com.google.cloud.batch.v1.Runnable;
import com.google.cloud.logging.LogEntryServerStream;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.*;
import io.kestra.plugin.gcp.GcpInterface;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Introspected
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Google Cloud Platform Batch script runner",
    description = """
        This job runner didn't resume the job if a Worker is restarted before the job finish.
        You need to have roles 'Batch Job Editor' and 'Logs Viewer' to be able to use it.""")
@Plugin(examples = {}, beta = true)
public class GcpBatchScriptRunner extends ScriptRunner implements GcpInterface {
    private static final String WORKING_DIR = "/kestra/working-dir";
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
        title = "Compute reservation."
    )
    @PluginProperty(dynamic = true)
    private String reservation;


    @Schema(
        title = "Google Cloud Storage Bucket to use to upload (`inputFiles` and `namespaceFiles`) and download (`outputFiles`) files.",
        description = "It's mandatory to provide a bucket if you want to use such properties."
    )
    @PluginProperty(dynamic = true)
    private String gcsBucket;

    @Schema(
        title = "The maximum duration to wait for the job completion. Google Cloud Batch will automatically timeout the job upon reaching such duration and the task will be failed."
    )
    @Builder.Default
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "Whether the job should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    private final Boolean delete = true;

    @Override
    public RunnerResult run(RunContext runContext, ScriptCommands commands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        String renderedBucket = runContext.render(gcsBucket);
        String workingDirName = IdUtils.create();
        Map<String, Object> additionalVars = commands.getAdditionalVars();
        Optional.ofNullable(renderedBucket).ifPresent(bucket -> additionalVars.putAll(Map.<String, Object>of(
            "gcsWorkingDir", "gs://" + bucket + "/" + workingDirName,
            "workingDir", WORKING_DIR,
            "outputDir", WORKING_DIR
        )));

        GoogleCredentials credentials = credentials(runContext);

        // TODO internal storage files?

        boolean hasFilesToUpload = !ListUtils.isEmpty(filesToUpload);
        if (hasFilesToUpload && gcsBucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        if (hasFilesToDownload && gcsBucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `outputFiles`");
        }

        if (hasFilesToUpload) {
            try (Storage storage = storage(runContext, credentials)) {
                for (String file: filesToUpload) {
                    BlobInfo destination = BlobInfo.newBuilder(BlobId.of(gcsBucket, workingDirName + "/" + file)).build();
                    try (var fileInputStream = new FileInputStream(runContext.resolve(Path.of(file)).toFile());
                        var writer = storage.writer(destination)) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int limit;
                        while ((limit = fileInputStream.read(buffer)) >= 0) {
                            writer.write(ByteBuffer.wrap(buffer, 0, limit));
                        }
                    }
                }
            }
        }

        try (BatchServiceClient batchServiceClient = BatchServiceClient.create(BatchServiceSettings.newBuilder().setCredentialsProvider(() -> credentials).build());
             Logging logging = LoggingOptions.getDefaultInstance().toBuilder().setCredentials(credentials).build().getService()) {
            var taskBuilder = TaskSpec.newBuilder();

            if (hasFilesToDownload || hasFilesToUpload) {
                taskBuilder.addVolumes(Volume.newBuilder()
                    .setGcs(GCS.newBuilder().setRemotePath(this.gcsBucket + "/" + workingDirName).build())
                    .setMountPath(MOUNT_PATH)
                    .build()
                );
            }

            // main container
            Runnable runnable =
                Runnable.newBuilder()
                    .setContainer(mainContainer(commands, hasFilesToDownload || hasFilesToUpload))
                    .setEnvironment(Environment.newBuilder()
                        .putAllVariables(commands.getEnv())
                        .putVariables("WORKING_DIR", WORKING_DIR)
                        .build()
                    )
                    .build();
            taskBuilder.addRunnables(runnable);

            TaskGroup taskGroup = TaskGroup.newBuilder().setTaskSpec(taskBuilder.build()).setTaskCount(1).build();

            // https://cloud.google.com/compute/docs/machine-types
            AllocationPolicy.InstancePolicy.Builder instancePolicy =
                AllocationPolicy.InstancePolicy.newBuilder()
                    .setMachineType(runContext.render(machineType));
            if (reservation != null) {
                instancePolicy.setReservation(runContext.render(reservation));
            }
            AllocationPolicy allocationPolicy =
                AllocationPolicy.newBuilder()
                    .addInstances(AllocationPolicy.InstancePolicyOrTemplate.newBuilder().setPolicy(instancePolicy).build())
                    .build();

            Job job =
                Job.newBuilder()
                    .addTaskGroups(taskGroup)
                    .setAllocationPolicy(allocationPolicy)
                    .putAllLabels(labels(runContext))
                    // We use Cloud Logging as it's an out of the box available option.
                    .setLogsPolicy(LogsPolicy.newBuilder().setDestination(LogsPolicy.Destination.CLOUD_LOGGING).build())
                    .build();

            CreateJobRequest createJobRequest =
                CreateJobRequest.newBuilder()
                    // The job's parent is the region in which the job will run.
                    .setParent(String.format("projects/%s/locations/%s", projectId, region))
                    .setJob(job)
                    .setJobId(jobId(runContext))
                    .build();

            Job result = batchServiceClient.createJob(createJobRequest);
            runContext.logger().info("Job created: " + result.getName());
            // Check for the job successful creation
            if (isFailed(result.getStatus().getState())) {
                throw new ScriptException(result.getStatus().getState().getNumber(), commands.getLogConsumer().getStdOutCount(), commands.getLogConsumer().getStdErrCount());
            }

            // if needed, batch infrastructure logs can be retrieved by using logName="projects/%s/logs/batch_task_logs" OR "%s/logs/batch_agent_logs"
            String logFilter = String.format(
                "logName=\"projects/%s/logs/batch_task_logs\" labels.job_uid=\"%s\"",
                projectId,
                result.getUid()
            );
            LogEntryServerStream stream = logging.tailLogEntries(Logging.TailOption.filter(logFilter));
            try (LogTail ignored = new LogTail(stream, commands.getLogConsumer())) {
                // Wait for the job termination
                result = waitFormTerminated(batchServiceClient, result);
                if (result == null) {
                    throw new TimeoutException();
                }
                if (isFailed(result.getStatus().getState())) {
                    throw new ScriptException(result.getStatus().getState().getNumber(), commands.getLogConsumer().getStdOutCount(), commands.getLogConsumer().getStdErrCount());
                }

                if (delete) {
                    batchServiceClient.deleteJobAsync(result.getName());
                    runContext.logger().info("Job deleted");
                }

                if (hasFilesToDownload) {
                    try (Storage storage = storage(runContext, credentials)) {
                        for (String file: filesToDownload) {
                            BlobInfo source = BlobInfo.newBuilder(BlobId.of(gcsBucket, workingDirName + "/" + file)).build();
                            try (var fileOutputStream = new FileOutputStream(runContext.resolve(Path.of(file)).toFile());
                                 var reader = storage.reader(source.getBlobId())) {
                                byte[] buffer = new byte[BUFFER_SIZE];
                                int limit;
                                while ((limit = reader.read(ByteBuffer.wrap(buffer))) >= 0) {
                                    fileOutputStream.write(buffer, 0, limit);
                                }
                            }
                        }
                    }
                }

                return new RunnerResult(0, commands.getLogConsumer());
            }
        } finally {
            if (hasFilesToUpload || hasFilesToDownload) {
                try (Storage storage = storage(runContext, credentials)) {
                    Page<Blob> list = storage.list(gcsBucket, Storage.BlobListOption.prefix(workingDirName));
                    list.iterateAll().forEach(blob -> storage.delete(blob.getBlobId()));
                    storage.delete(BlobInfo.newBuilder(BlobId.of(gcsBucket, workingDirName)).build().getBlobId());
                }
            }
        }
    }

    private Runnable.Container mainContainer(ScriptCommands commands, boolean mountVolume) {
        // TODO working directory
        var builder =  Runnable.Container.newBuilder()
            .setImageUri(commands.getContainerImage())
            .addAllCommands(commands.getCommands());

        if (mountVolume) {
            builder.addVolumes(MOUNT_PATH + ":" + WORKING_DIR);
        }

        return builder.build();
    }

    private Job waitFormTerminated(BatchServiceClient batchServiceClient, Job result) throws TimeoutException {
        return Await.until(
            () -> {
                Job terminated = batchServiceClient.getJob(result.getName());
                if (isTerminated(terminated.getStatus().getState())) {
                    return terminated;
                }
                return null;
            },
            Duration.ofMillis(500),
            this.waitUntilCompletion
        );
    }

    private boolean isFailed(JobStatus.State state) {
        return state == JobStatus.State.STATE_UNSPECIFIED || state == JobStatus.State.FAILED || state == JobStatus.State.UNRECOGNIZED;
    }

    private boolean isTerminated(JobStatus.State state) {
        return state ==  JobStatus.State.SUCCEEDED || state ==  JobStatus.State.DELETION_IN_PROGRESS || isFailed(state);
    }



    // validation regex: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$.
    // FIXME duplicated with the k8s runner
    private String jobId(RunContext runContext) {
        Map<String, String> flow = (Map<String, String>) runContext.getVariables().get("flow");
        Map<String, String> task = (Map<String, String>) runContext.getVariables().get("task");

        String name = Slugify.of(String.join(
            "-",
            flow.get("namespace"),
            flow.get("id"),
            task.get("id")
        ));
        String normalized = normalizedValue(name);
        if (normalized.length() > 58) {
            normalized = normalized.substring(0, 57);
        }

        // we add a suffix of 5 chars, this should be enough as it's the standard k8s way
        String suffix = RandomStringUtils.randomAlphanumeric(5).toLowerCase();
        return normalized + "-" + suffix;
    }

    // FIXME duplicated with the k8s runner
    private String normalizedValue(String name) {
        if (name.length() > 63) {
            name = name.substring(0, 63);
        }

        name = StringUtils.stripEnd(name, "-");
        name = StringUtils.stripEnd(name, ".");
        name = StringUtils.stripEnd(name, "_");

        return name.toLowerCase();
    }

    // FIXME duplicated with the k8s runner
    private Map<String, String> labels(RunContext runContext) {
        Map<String, String> flow = (Map<String, String>) runContext.getVariables().get("flow");
        Map<String, String> task = (Map<String, String>) runContext.getVariables().get("task");
        Map<String, String> execution = (Map<String, String>) runContext.getVariables().get("execution");
        Map<String, String> taskrun = (Map<String, String>) runContext.getVariables().get("taskrun");

        return ImmutableMap.of(
            "namespace", normalizedValue(flow.get("namespace")),
            "low-id", normalizedValue(flow.get("id")),
            "task-id", normalizedValue(task.get("id")),
            "execution-id", normalizedValue(execution.get("id")),
            "taskrun-id", normalizedValue(taskrun.get("id")),
            "taskrun-attempt", normalizedValue(String.valueOf(taskrun.get("attemptsCount")))
        );
    }

    // TODO duplicated with Cloud Storage tasks
    private Storage storage(RunContext runContext, GoogleCredentials credentials) throws IllegalVariableEvaluationException {
        VersionProvider versionProvider = runContext.getApplicationContext().getBean(VersionProvider.class);

        return StorageOptions
            .newBuilder()
            .setCredentials(credentials)
            .setProjectId(runContext.render(projectId))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + versionProvider.getVersion()))
            .build()
            .getService();
    }

    // TODO duplicated with AbstractTask
    private GoogleCredentials credentials(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        GoogleCredentials credentials;


        if (serviceAccount != null) {
            String serviceAccount = runContext.render(this.serviceAccount);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serviceAccount.getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);
            Logger logger = runContext.logger();

            if (logger.isTraceEnabled()) {
                byteArrayInputStream.reset();
                Map<String, String> jsonKey = JacksonMapper.ofJson().readValue(byteArrayInputStream, new TypeReference<>() {});
                if (jsonKey.containsKey("client_email")) {
                    logger.trace(" • Using service account: {}", jsonKey.get("client_email") );
                }
            }
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        if (this.scopes != null) {
            credentials = credentials.createScoped(runContext.render(this.scopes));
        }

        return credentials;
    }
}
