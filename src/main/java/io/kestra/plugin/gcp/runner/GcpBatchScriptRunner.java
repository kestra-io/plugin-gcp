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
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.*;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.plugin.gcp.GcpInterface;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
    public RunnerResult run(RunContext runContext, ScriptCommands scriptCommands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        String renderedBucket = runContext.render(bucket);
        String workingDirName = IdUtils.create();
        Map<String, Object> additionalVars = scriptCommands.getAdditionalVars();
        // TODO outputDir
        Optional.ofNullable(renderedBucket).ifPresent(bucket -> additionalVars.putAll(Map.<String, Object>of(
            ScriptService.VAR_BUCKET_PATH, "gs://" + bucket + "/" + workingDirName,
            ScriptService.VAR_WORKING_DIR, WORKING_DIR
        )));

        GoogleCredentials credentials = CredentialService.credentials(runContext, this);

        List<String> allFilesToUpload = new ArrayList<>(ListUtils.emptyOnNull(filesToUpload));
        List<String> command = ScriptService.uploadInputFiles(
            runContext,
            runContext.render(scriptCommands.getCommands(), additionalVars),
            (ignored, localFilePath) -> allFilesToUpload.add(localFilePath),
            true
        );

        boolean hasFilesToUpload = !ListUtils.isEmpty(allFilesToUpload);
        if (hasFilesToUpload && bucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        if (hasFilesToDownload && bucket == null) {
            throw new IllegalArgumentException("You must provide a Cloud Storage Bucket to use `outputFiles`");
        }

        if (hasFilesToUpload) {
            try (Storage storage = storage(runContext, credentials)) {
                for (String file: allFilesToUpload) {
                    BlobInfo destination = BlobInfo.newBuilder(BlobId.of(bucket, workingDirName + "/" + file)).build();
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
                    .setGcs(GCS.newBuilder().setRemotePath(this.bucket + "/" + workingDirName).build())
                    .setMountPath(MOUNT_PATH)
                    .build()
                );
            }

            // main container
            Map<String, String> environment = new HashMap<>(runContext.renderMap(scriptCommands.getEnv(), additionalVars));
            environment.put(ScriptService.ENV_BUCKET_PATH, this.bucket + "/" + workingDirName);
            environment.put(ScriptService.ENV_WORKING_DIR, WORKING_DIR);
            // TODO outputDir
//        environment.put(ScriptService.ENV_OUTPUT_DIR, scriptCommands.getOutputDirectory().toString());
            Runnable runnable =
                Runnable.newBuilder()
                    .setContainer(mainContainer(scriptCommands, command, hasFilesToDownload || hasFilesToUpload))
                    .setEnvironment(Environment.newBuilder()
                        .putAllVariables(environment)
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
                    .putAllLabels(ScriptService.labels(runContext, "kestra-", true, true))
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

            Job result = batchServiceClient.createJob(createJobRequest);
            runContext.logger().info("Job created: " + result.getName());
            // Check for the job successful creation
            if (isFailed(result.getStatus().getState())) {
                throw new ScriptException(result.getStatus().getState().getNumber(), scriptCommands.getLogConsumer().getStdOutCount(), scriptCommands.getLogConsumer().getStdErrCount());
            }

            // if needed, batch infrastructure logs can be retrieved by using logName="projects/%s/logs/batch_task_logs" OR "%s/logs/batch_agent_logs"
            String logFilter = String.format(
                "logName=\"projects/%s/logs/batch_task_logs\" labels.job_uid=\"%s\"",
                projectId,
                result.getUid()
            );
            LogEntryServerStream stream = logging.tailLogEntries(Logging.TailOption.filter(logFilter));
            try (LogTail ignored = new LogTail(stream, scriptCommands.getLogConsumer())) {
                // Wait for the job termination
                result = waitFormTerminated(batchServiceClient, result);
                if (result == null) {
                    throw new TimeoutException();
                }
                if (isFailed(result.getStatus().getState())) {
                    throw new ScriptException(result.getStatus().getState().getNumber(), scriptCommands.getLogConsumer().getStdOutCount(), scriptCommands.getLogConsumer().getStdErrCount());
                }

                if (delete) {
                    batchServiceClient.deleteJobAsync(result.getName());
                    runContext.logger().info("Job deleted");
                }

                if (hasFilesToDownload) {
                    try (Storage storage = storage(runContext, credentials)) {
                        for (String file: filesToDownload) {
                            BlobInfo source = BlobInfo.newBuilder(BlobId.of(bucket, workingDirName + "/" + file)).build();
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

                return new RunnerResult(0, scriptCommands.getLogConsumer());
            }
        } finally {
            if (hasFilesToUpload || hasFilesToDownload) {
                try (Storage storage = storage(runContext, credentials)) {
                    Page<Blob> list = storage.list(bucket, Storage.BlobListOption.prefix(workingDirName));
                    list.iterateAll().forEach(blob -> storage.delete(blob.getBlobId()));
                    storage.delete(BlobInfo.newBuilder(BlobId.of(bucket, workingDirName)).build().getBlobId());
                }
            }
        }
    }

    private Runnable.Container mainContainer(ScriptCommands scriptCommands, List<String> command, boolean mountVolume) {
        // TODO working directory
        var builder =  Runnable.Container.newBuilder()
            .setImageUri(scriptCommands.getContainerImage())
            .addAllCommands(command);

        if (mountVolume) {
            builder.addVolumes(MOUNT_PATH + ":" + WORKING_DIR);
        }

        if (this.entryPoint != null) {
            builder.setEntrypoint(String.join(" ", this.entryPoint));
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

    @Getter
    @Builder
    public static class NetworkInterface {
        @Schema(title = "Network identifier with the format `projects/HOST_PROJECT_ID/global/networks/NETWORK`.")
        @PluginProperty(dynamic = true)
        @NotNull
        private String network;

        @Schema(title = "Subnetwork identifier with the format `projects/HOST_PROJECT_ID/regions/REGION/subnetworks/SUBNET`")
        @PluginProperty(dynamic = true)
        private String subnetwork;
    }
}
