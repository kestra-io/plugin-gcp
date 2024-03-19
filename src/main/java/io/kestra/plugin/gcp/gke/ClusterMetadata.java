package io.kestra.plugin.gcp.gke;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.container.v1.Cluster;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Fetch a GKE cluster's metadata.",
            code = {
                "clusterProjectId: my-project-id",
                "clusterZone: europe-west1-c",
                "clusterId: my-cluster-id",
            }
        )
    }
)
@Schema(
    title = "Get GKE cluster's metadata."
)
public class ClusterMetadata extends AbstractTask implements RunnableTask<ClusterMetadata.Output> {
    @NotNull
    @Schema(
        title = "Cluster ID whose metadata needs to be fetched."
    )
    @PluginProperty(dynamic = true)
    private String clusterId;

    @Schema(
        title = "GCP zone in which the GKE cluster is present."
    )
    @PluginProperty(dynamic = true)
    private String clusterZone;

    @Schema(
        title = "GCP project ID where the GKE cluster is present."
    )
    @PluginProperty(dynamic = true)
    private String clusterProjectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Cluster cluster = fetch(runContext);
        return Output
            .builder()
            .location(cluster.getLocation())
            .description(cluster.getDescription())
            .network(cluster.getNetwork())
            .name(cluster.getName())
            .clusterIpv4Cidr(cluster.getClusterIpv4Cidr())
            .subNetwork(cluster.getSubnetwork())
            .endpoint(cluster.getEndpoint())
            .zone(clusterZone)
            .project(clusterProjectId)
            .createTime(cluster.getCreateTime())
            .nodePoolsCount(cluster.getNodePoolsCount())
            .nodePools(cluster.getNodePoolsList()
                .stream()
                .map(r -> NodePool.builder()
                    .name(r.getName())
                    .status(r.getStatus())
                    .build()
                )
                .collect(Collectors.toList())
            )
            .masterAuth(MasterAuth.builder()
                .clusterCertificat(cluster.getMasterAuth().getClusterCaCertificate())
                .clientKey(cluster.getMasterAuth().getClientKey())
                .clientCertificat(cluster.getMasterAuth().getClientCertificate())
                .build()
            )
            .loggingService(cluster.getLoggingService())
            .monitoringService(cluster.getMonitoringService())
            .link(cluster.getSelfLink())
            .build();
    }

    Cluster fetch(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        ClusterManagerSettings clusterManagerSettings = ClusterManagerSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .build();

        try (ClusterManagerClient client = ClusterManagerClient.create(clusterManagerSettings)) {
            return client.getCluster(
                runContext.render(clusterProjectId),
                runContext.render(clusterZone),
                runContext.render(clusterId)
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final String location;
        private final String network;
        private final String name;
        private final String description;
        private final String clusterIpv4Cidr;
        private final String subNetwork;
        private final String endpoint;
        private final String zone;
        private final String project;
        private final String createTime;
        private final int nodePoolsCount;
        private final List<NodePool> nodePools;
        private final MasterAuth masterAuth;
        private final String loggingService;
        private final String monitoringService;
        private final String link;
    }

    @Builder
    @Getter
    public static class MasterAuth {
        private final String clusterCertificat;
        private final String clientKey;
        private final String clientCertificat;
    }

    @Builder
    @Getter
    public static class NodePool {
        private final String name;
        private final com.google.container.v1.NodePool.Status status;
    }
}
