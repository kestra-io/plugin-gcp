package io.kestra.plugin.gcp.dataproc.clusters;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create clusters in Google Cloud Dataproc."
)
@Plugin(
    examples = {
        @Example(
            title = "Creates a cluster in Google Cloud Dataproc.",
            full = true,
            code = """
                id: gcp_dataproc_cluster_create
                namespace: company.team

                tasks:
                  - id: cluster_create
                    type: io.kestra.plugin.gcp.dataproc.clusters.Create
                    clusterName: YOUR_CLUSTER_NAME
                    region: YOUR_REGION
                    zone: YOUR_ZONE
                    masterMachineType: n1-standard-2
                    workerMachineType: n1-standard-2
                    workers: 2
                    bucket: YOUR_BUCKET_NAME
                """
        ),
        @Example(
            title = "Creates a cluster in Google Cloud Dataproc with specific disk size.",
            full = true,
            code = """
                id: gcp_dataproc_cluster_create
                namespace: company.team

                tasks:
                  - id: create_cluster_with_certain_disk_size
                    type: io.kestra.plugin.gcp.dataproc.clusters.Create
                    clusterName: YOUR_CLUSTER_NAME
                    region: YOUR_REGION
                    zone: YOUR_ZONE
                    masterMachineType: n1-standard-2
                    masterDiskSizeGB: 500
                    workerMachineType: n1-standard-2
                    workerDiskSizeGB: 200
                    workers: 2
                    bucket: YOUR_BUCKET_NAM
                """
        )
    }
)
public class Create extends AbstractTask implements RunnableTask<Create.Output> {

    public static final String DATAPROC_GOOGLEAPIS = "-dataproc.googleapis.com:443";

    @Schema(
        title = "The cluster name."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String clusterName;

    @Schema(
        title = "The region."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String region;

    @Schema(
        title = "The zone."
    )
    @PluginProperty(dynamic = true)
    private String zone;

    @Schema(
        title = "The master machine type."
    )
    @PluginProperty(dynamic = true)
    private String masterMachineType;

    @Schema(
        title = "The disk size in GB for each master node."
    )
    @PluginProperty
    private Integer masterDiskSizeGB;

    @Schema(
        title = "The worker machine type."
    )
    @PluginProperty(dynamic = true)
    private String workerMachineType;

    @Schema(
        title = "The disk size in GB for each worker node."
    )
    @PluginProperty
    private Integer workerDiskSizeGB;

    @Schema(
        title = "The number of workers."
    )
    @PluginProperty
    private Integer workers;

    @Schema(
        title = "The GCS bucket name."
    )
    @PluginProperty(dynamic = true)
    private String bucket;

    @Schema(
        title = "The Dataproc image URI.",
        description = "The Compute Engine image resource used for cluster instances."
    )
    @PluginProperty(dynamic = true)
    private String imageVersion;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String region = runContext.render(this.region);
        String clusterName = runContext.render(this.clusterName);

        ClusterControllerSettings settings = ClusterControllerSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .setEndpoint(region + DATAPROC_GOOGLEAPIS)
            .build();

        try (ClusterControllerClient client = ClusterControllerClient.create(settings)) {
            ClusterConfig config = getConfig(runContext);

            Cluster cluster = Cluster.newBuilder()
                .setClusterName(clusterName)
                .setConfig(config)
                .build();

            CreateClusterRequest request = CreateClusterRequest.newBuilder()
                .setProjectId(runContext.render(this.projectId).as(String.class).orElse(null))
                .setRegion(region)
                .setCluster(cluster)
                .build();

            OperationFuture<Cluster, ClusterOperationMetadata> clusterAsync = client.createClusterAsync(request);

            clusterAsync.get();
            ClusterOperationMetadata metadata = clusterAsync.getMetadata().get();

            if (metadata == null) {
                return Output.builder()
                    .clusterName(clusterName)
                    .created(true)
                    .build();
            }

            return Output.builder()
                .clusterName(metadata.getClusterName())
                .created(metadata.getStatus().getInnerState().equalsIgnoreCase("DONE"))
                .build();
        }
    }

    private ClusterConfig getConfig(RunContext runContext) throws IllegalVariableEvaluationException {
        ClusterConfig.Builder configBuilder = ClusterConfig.newBuilder();

        if (this.masterMachineType != null) {
            configBuilder.setMasterConfig(
                configureMachine(runContext, this.masterMachineType, this.masterDiskSizeGB, null)
            );
        }

        if (this.workerMachineType != null) {
            configBuilder.setWorkerConfig(
                configureMachine(runContext, this.workerMachineType, this.workerDiskSizeGB, this.workers)
            );
        }

        if (this.zone != null) {
            configBuilder.setGceClusterConfig(
                GceClusterConfig.newBuilder()
                    .setZoneUri(runContext.render(this.zone))
                    .build()
            );
        }

        if (this.bucket != null) {
            configBuilder.setConfigBucket(runContext.render(this.bucket));
        }

        return configBuilder.build();
    }

    private InstanceGroupConfig configureMachine(
        RunContext runContext,
        String machineType,
        Integer diskSizeGB,
        Integer workers
    ) throws IllegalVariableEvaluationException {
        InstanceGroupConfig.Builder instanceGroupConfigBuilder = InstanceGroupConfig.newBuilder()
            .setMachineTypeUri(runContext.render(machineType));

        if (diskSizeGB != null) {
            instanceGroupConfigBuilder
                .setDiskConfig(
                    DiskConfig.newBuilder()
                        .setBootDiskSizeGb(diskSizeGB)
                        .build()
                );
        }

        if (this.imageVersion != null) {
            instanceGroupConfigBuilder
                .setImageUri(runContext.render(this.imageVersion));
        }

        if (workers != null) {
            instanceGroupConfigBuilder
                .setNumInstances(workers);
        }

        return instanceGroupConfigBuilder.build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The cluster name."
        )
        private String clusterName;

        @Schema(
            title = "Whether cluster was created successfully."
        )
        private boolean created;

    }

}
