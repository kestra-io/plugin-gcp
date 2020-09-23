package org.kestra.task.gcp.gke;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.container.v1.ClientCertificateConfig;
import com.google.container.v1.Cluster;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;
import org.kestra.task.gcp.bigquery.AbstractBigquery;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Get a cluster meta data set",
    code = {
        "clusterId: \"\"",
        "deleteContents: true"
    }
)
@Documentation(
    description = "Delete a dataset."
)
public class GkeMetas extends Task implements RunnableTask<GkeMetas.Output> {
    @NotNull
    @InputProperty(
        description = "Cluster id where meta data are fetch"
    )
    private String clusterId;

    @InputProperty(
        description = "Cluster zone in GCP"
    )
    private String zone;

    @InputProperty(
        description = "Project ID in GCP were is located cluster"
    )
    private String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        ClusterManagerSettings clusterManagerSettings = ClusterManagerSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
                .build();

        try (ClusterManagerClient client = ClusterManagerClient.create(clusterManagerSettings)) {
            Cluster cluster = client.getCluster(projectId, zone, clusterId);

            ClusterMetaData clusterMetaData = ClusterMetaData.builder()
                .location(cluster.getLocation())
                .description(cluster.getDescription())
                .network(cluster.getNetwork())
                .name(cluster.getName())
                .ipV4(cluster.getClusterIpv4Cidr())
                .subNetwork(cluster.getSubnetwork())
                .endpoint(cluster.getEndpoint())
                .zone(zone)
                .project(projectId)
                .createTime(cluster.getCreateTime())
                .nodeCount(cluster.getNodePoolsCount())
                .serviceAccount(cluster.getNodeConfig().getServiceAccount())
                .machineType(cluster.getNodeConfig().getMachineType())
                .username(cluster.getMasterAuth().getUsername())
                .password(cluster.getMasterAuth().getPassword())
                .clusterCertificat(cluster.getMasterAuth().getClusterCaCertificate())
                .clientCertificat(cluster.getMasterAuth().getClientCertificate())
                .clientCertificatConfig(cluster.getMasterAuth().getClientCertificateConfig())
                .loggingService(cluster.getLoggingService())
                .monitoringService(cluster.getMonitoringService())
                .link(cluster.getSelfLink())
                .build();

            return Output
                .builder()
                .clusterMetaData(clusterMetaData)
                .build();
        }
    }

    @Builder
    @Getter
    public static class ClusterMetaData {
        private String location;
        private String network;
        private String name;
        private String description;
        private String ipV4;
        private String subNetwork;
        private String endpoint;
        private String zone;
        private String project;
        private String createTime;
        private int nodeCount;
        private String serviceAccount;
        private String machineType;
        private String username;
        private String password;
        private String clusterCertificat;
        private String clientKey;
        private String clientCertificat;
        private ClientCertificateConfig clientCertificatConfig;
        private String loggingService;
        private String monitoringService;
        private String link;
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @NotNull
        @OutputProperty(
            description = "A cluster token to control it"
        )
        private ClusterMetaData clusterMetaData;
    }
}
