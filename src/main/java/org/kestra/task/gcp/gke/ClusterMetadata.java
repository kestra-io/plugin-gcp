package org.kestra.task.gcp.gke;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.container.v1.Cluster;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Fetch a gke cluster metadata",
    code = {
        "name: \"gke-metas\"",
        "projectId: my-project-id",
        "zone: eu-west-1c",
        "clusterId: my-cluster-id",
    }
)
@Documentation(
    description = "Delete a dataset."
)
public class ClusterMetadata extends Task implements RunnableTask<ClusterMetadata.Output> {
    @NotNull
    @InputProperty(
        description = "Cluster id where meta data are fetch",
        dynamic = true
    )
    private String clusterId;

    @InputProperty(
        description = "Cluster zone in GCP",
        dynamic = true
    )
    private String zone;

    @InputProperty(
        description = "Project ID in GCP were is located cluster",
        dynamic = true
    )
    private String projectId;

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
            .zone(zone)
            .project(projectId)
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
                .username(cluster.getMasterAuth().getUsername())
                .password(cluster.getMasterAuth().getPassword())
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
            .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
            .build();

        try (ClusterManagerClient client = ClusterManagerClient.create(clusterManagerSettings)) {
            return client.getCluster(
                runContext.render(projectId),
                runContext.render(zone),
                runContext.render(clusterId)
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
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
        @OutputProperty(
            description = "The username to use for HTTP basic authentication to the master endpoint.",
            body = {
                "For clusters v1.6.0 and later, basic authentication can be disabled by",
                "leaving username unspecified (or setting it to the empty string)."
            }
        )
        private final String username;
        private final String password;
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
