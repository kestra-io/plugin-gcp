package io.kestra.plugin.gcp.dataproc.batches;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.dataproc.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Slugify;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBatch extends AbstractTask implements RunnableTask<AbstractBatch.Output> {
    @Schema(
        title = "The region"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String region;

    @Schema(
        title = "The batch name"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String name;

    @Schema(
        title = "Execution configuration for a workload."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private AbstractBatch.ExecutionConfiguration execution;

    @Schema(
        title = "Execution configuration for a workload."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private AbstractBatch.PeripheralsConfiguration peripherals;

    @Schema(
        title = "Execution configuration for a workload."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private AbstractBatch.RuntimeConfiguration runtime;

    abstract protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String region = runContext.render(this.region);
        String batchId = Slugify.of(runContext.render(name + "-{{ execution.id}}"));
        if (batchId.length() > 63) {
            batchId = batchId.substring(0, 63);
        }

        BatchControllerSettings batchControllerSettings = BatchControllerSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .setEndpoint(region + "-dataproc.googleapis.com:443")
            .build();

        try (BatchControllerClient batchControllerClient = BatchControllerClient.create(batchControllerSettings)) {
            Batch.Builder builder = Batch.newBuilder();
            this.buildBatch(builder, runContext);

            ExecutionConfig.Builder executionConfig = ExecutionConfig.newBuilder();

            if (this.execution.networkUri != null) {
                executionConfig.setNetworkUri(runContext.render(this.execution.networkUri));
            }

            if (this.execution.subnetworkUri != null) {
                executionConfig.setSubnetworkUri(runContext.render(this.execution.subnetworkUri));
            }

            if (this.execution.networkTags != null) {
                for (int i = 0; i < this.execution.networkTags.size(); i++) {
                    executionConfig.setNetworkTags(i, runContext.render(this.execution.networkTags.get(i)));
                }
            }

            if (this.execution.serviceAccountEmail != null) {
                executionConfig.setServiceAccount(runContext.render(this.execution.serviceAccountEmail));
            }

            if (this.execution.kmsKey != null) {
                executionConfig.setKmsKey(runContext.render(this.execution.kmsKey));
            }

            EnvironmentConfig.Builder environmentConfig = EnvironmentConfig.newBuilder()
                .setExecutionConfig(executionConfig.build());

            if (this.peripherals != null) {
                PeripheralsConfig.Builder peripheralsConfig = PeripheralsConfig.newBuilder();

                if (this.peripherals.metastoreService != null) {
                    peripheralsConfig.setMetastoreService(runContext.render(this.peripherals.metastoreService));
                }

                if (this.peripherals.sparkHistoryServer != null) {
                    peripheralsConfig.setSparkHistoryServerConfig(SparkHistoryServerConfig.newBuilder()
                        .setDataprocCluster(runContext.render(this.peripherals.sparkHistoryServer.dataprocCluster))
                        .build()
                    );
                }

                environmentConfig.setPeripheralsConfig(peripheralsConfig.build());
            }

            Batch.Builder batchBuilder = builder.setEnvironmentConfig(environmentConfig.build());

            if (this.runtime != null) {
                RuntimeConfig.Builder runtimeConfig = RuntimeConfig.newBuilder();

                if (this.runtime.containerImage != null) {
                    runtimeConfig.setContainerImage(runContext.render(this.runtime.containerImage));
                }

                if (this.runtime.version != null) {
                    runtimeConfig.setVersion(runContext.render(this.runtime.version));
                }

                if (this.runtime.properties != null) {
                    runtimeConfig.putAllProperties(this.runtime.properties
                        .entrySet()
                        .stream()
                        .map(throwFunction(entry -> new AbstractMap.SimpleEntry<>(
                            runContext.render(entry.getKey()),
                            runContext.render(entry.getValue())
                        )))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    );
                }

                batchBuilder.setRuntimeConfig(runtimeConfig.build());
            }


            batchBuilder.setEnvironmentConfig(EnvironmentConfig.newBuilder().build());

            Batch batch = batchBuilder.build();

            logger.info("Starting with batch id '{}'", batchId);

            CreateBatchRequest request = CreateBatchRequest.newBuilder()
                .setParent(LocationName.of(runContext.render(this.projectId), region).toString())
                .setBatch(batch)
                .setBatchId(batchId)
                .build();
            Batch response = batchControllerClient.createBatchAsync(request).get();

            return Output.builder().state(response.getState()).build();
        }
    }

    @Builder
    @Getter
    public static class ExecutionConfiguration {
        @Schema(
            title = "Network URI to connect workload to."
        )
        @PluginProperty(dynamic = true)
        private String networkUri;

        @Schema(
            title = "Subnetwork URI to connect workload to."
        )
        @PluginProperty(dynamic = true)
        private String subnetworkUri;

        @Schema(
            title = "Tags used for network traffic control."
        )
        @PluginProperty(dynamic = true)
        private List<String> networkTags;

        @Schema(
            title = "Service account that used to execute workload."
        )
        @PluginProperty(dynamic = true)
        private String serviceAccountEmail;

        @Schema(
            title = "The Cloud KMS key to use for encryption."
        )
        @PluginProperty(dynamic = true)
        private String kmsKey;
    }

    @Builder
    @Getter
    public static class PeripheralsConfiguration {
        @Schema(
            title = "Resource name of an existing Dataproc Metastore service.",
            description = "Example: `projects/[project_id]/locations/[region]/services/[service_id]`"
        )
        @PluginProperty(dynamic = true)
        private String metastoreService;

        @Schema(
            title = "Resource name of an existing Dataproc Metastore service.",
            description = "Example: `projects/[project_id]/locations/[region]/services/[service_id]`"
        )
        @PluginProperty(dynamic = true)
        private SparkHistoryServerConfiguration sparkHistoryServer;
    }

    @Builder
    @Getter
    public static class SparkHistoryServerConfiguration {
        @Schema(
            title = "Resource name of an existing Dataproc Cluster to act as a Spark History Server for the workload.",
            description = "Example: `projects/[project_id]/regions/[region]/clusters/[cluster_name]`"
        )
        @PluginProperty(dynamic = true)
        private String dataprocCluster;
    }

    @Builder
    @Getter
    public static class RuntimeConfiguration {
        @Schema(
            title = "Optional custom container image for the job runtime environment.",
            description = "If not specified, a default container image will be used."
        )
        @PluginProperty(dynamic = true)
        private String containerImage;

        @Schema(
            title = "Version of the batch runtime."
        )
        @PluginProperty(dynamic = true)
        private String version;

        @Schema(
            title = " mapping of property names to values, which are used to configure workload execution."
        )
        @PluginProperty(dynamic = true)
        private Map<String, String> properties;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The state of the batch."
        )
        private final com.google.cloud.dataproc.v1.Batch.State state;
    }
}
