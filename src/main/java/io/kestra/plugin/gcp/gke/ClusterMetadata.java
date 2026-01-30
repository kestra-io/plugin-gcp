package io.kestra.plugin.gcp.gke;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.container.v1.Cluster;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
            full = true,
            code = """
                id: gcp_gke_cluster_metadata
                namespace: company.team

                tasks:
                  - id: cluster_metadata
                    type: io.kestra.plugin.gcp.gke.ClusterMetadata
                    clusterProjectId: my-project-id
                    clusterZone: europe-west1-c
                    clusterId: my-cluster-id
                """
        )
    }
)
@Schema(
    title = "Fetch GKE cluster metadata",
    description = "Retrieves metadata for a GKE cluster in the specified project and zone/region, including networking, auth certs, and node pool statuses."
)
public class ClusterMetadata extends AbstractTask implements RunnableTask<ClusterMetadata.Output> {
    @NotNull
    @Schema(
        title = "Cluster ID",
        description = "Name of the cluster to query"
    )
    private Property<String> clusterId;

    @Schema(
        title = "Cluster location",
        description = "Zone or region where the cluster resides"
    )
    private Property<String> clusterZone;

    @Schema(
        title = "Cluster project ID",
        description = "Project that owns the cluster"
    )
    private Property<String> clusterProjectId;

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
            .zone(runContext.render(clusterZone).as(String.class).orElse(null))
            .project(runContext.render(clusterProjectId).as(String.class).orElse(null))
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
                .clusterCertificate(cluster.getMasterAuth().getClusterCaCertificate())
                .clientKey(cluster.getMasterAuth().getClientKey())
                .clientCertificate(cluster.getMasterAuth().getClientCertificate())
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
            return client.getCluster("projects/%s/locations/%s/clusters/%s".formatted(
                runContext.render(clusterProjectId).as(String.class).orElse(null),
                runContext.render(clusterZone).as(String.class).orElse(null),
                runContext.render(clusterId).as(String.class).orElse(null)
            ));
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
        private final String clusterCertificate;

        @Deprecated
        public String getClusterCertificat() {
            return clusterCertificate;
        }

        private final String clientKey;
        private final String clientCertificate;

        @Deprecated
        public String getClientCertificat() {
            return clientCertificate;
        }
    }

    @Builder
    @Getter
    public static class NodePool {
        private final String name;
        private final com.google.container.v1.NodePool.Status status;
    }
}
