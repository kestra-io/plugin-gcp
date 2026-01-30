package io.kestra.plugin.gcp.dataproc.batches;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.dataproc.v1.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBatch extends AbstractTask implements RunnableTask<AbstractBatch.Output> {
    @Schema(
        title = "Dataproc region",
        description = "Region endpoint used for the batch controller"
    )
    @NotNull
    private Property<String> region;

    @Schema(
        title = "Batch name",
        description = "Base name; a slugified suffix with execution id is appended and truncated to 63 chars"
    )
    @NotNull
    private Property<String> name;

    @Schema(
        title = "Execution configuration",
        description = "Network, service account, and KMS settings for the workload"
    )
    @PluginProperty(dynamic = true)
    private AbstractBatch.ExecutionConfiguration execution;

    @Schema(
        title = "Peripherals configuration",
        description = "Optional Metastore and Spark History Server settings"
    )
    @PluginProperty(dynamic = true)
    private AbstractBatch.PeripheralsConfiguration peripherals;

    @Schema(
        title = "Runtime configuration",
        description = "Container image, runtime version, and properties"
    )
    @PluginProperty(dynamic = true)
    private AbstractBatch.RuntimeConfiguration runtime;

    abstract protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String region = runContext.render(this.region).as(String.class).orElseThrow();
        String batchId = Slugify.of(runContext.render(runContext.render(name).as(String.class).orElseThrow() + "-{{ execution.id}}"));
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

            if (this.execution != null) {
                if (this.execution.networkUri != null) {
                    executionConfig.setNetworkUri(runContext.render(this.execution.networkUri).as(String.class).orElseThrow());
                }

                if (this.execution.subnetworkUri != null) {
                    executionConfig.setSubnetworkUri(runContext.render(this.execution.subnetworkUri).as(String.class).orElseThrow());
                }

                var tagsList = runContext.render(this.execution.networkTags).asList(String.class);
                if (!tagsList.isEmpty()) {
                    for (int i = 0; i < tagsList.size(); i++) {
                        executionConfig.setNetworkTags(i, tagsList.get(i));
                    }
                }

                if (this.execution.serviceAccountEmail != null) {
                    executionConfig.setServiceAccount(runContext.render(this.execution.serviceAccountEmail).as(String.class).orElseThrow());
                }

                if (this.execution.kmsKey != null) {
                    executionConfig.setKmsKey(runContext.render(this.execution.kmsKey).as(String.class).orElseThrow());
                }
            }

            EnvironmentConfig.Builder environmentConfig = EnvironmentConfig.newBuilder()
                .setExecutionConfig(executionConfig.build());

            if (this.peripherals != null) {
                PeripheralsConfig.Builder peripheralsConfig = PeripheralsConfig.newBuilder();

                if (this.peripherals.metastoreService != null) {
                    peripheralsConfig.setMetastoreService(runContext.render(this.peripherals.metastoreService).as(String.class).orElseThrow());
                }

                if (this.peripherals.sparkHistoryServer != null) {
                    peripheralsConfig.setSparkHistoryServerConfig(SparkHistoryServerConfig.newBuilder()
                        .setDataprocCluster(runContext.render(this.peripherals.sparkHistoryServer.dataprocCluster).as(String.class).orElseThrow())
                        .build()
                    );
                }

                environmentConfig.setPeripheralsConfig(peripheralsConfig.build());
            }

            Batch.Builder batchBuilder = builder.setEnvironmentConfig(environmentConfig.build());

            if (this.runtime != null) {
                RuntimeConfig.Builder runtimeConfig = RuntimeConfig.newBuilder();

                if (this.runtime.containerImage != null) {
                    runtimeConfig.setContainerImage(runContext.render(this.runtime.containerImage).as(String.class).orElseThrow());
                }

                if (this.runtime.version != null) {
                    runtimeConfig.setVersion(runContext.render(this.runtime.version).as(String.class).orElseThrow());
                }

                var runtimeProps = runContext.render(this.runtime.properties).asMap(String.class, String.class);
                if (!runtimeProps.isEmpty()) {
                    runtimeConfig.putAllProperties(runtimeProps
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
                .setParent(LocationName.of(runContext.render(this.projectId).as(String.class).orElse(null), region).toString())
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
            title = "Network URI",
            description = "VPC network URI for the workload"
        )
        private Property<String> networkUri;

        @Schema(
            title = "Subnetwork URI",
            description = "Optional subnetwork URI for the workload"
        )
        private Property<String> subnetworkUri;

        @Schema(
            title = "Network tags",
            description = "Applied to workload for network control"
        )
        private Property<List<String>> networkTags;

        @Schema(
            title = "Service account",
            description = "Email of the service account used to run the batch"
        )
        private Property<String> serviceAccountEmail;

        @Schema(
            title = "KMS key",
            description = "Cloud KMS key for encryption"
        )
        private Property<String> kmsKey;
    }

    @Builder
    @Getter
    public static class PeripheralsConfiguration {
        @Schema(
            title = "Metastore service",
            description = "Resource name of an existing Dataproc Metastore service (projects/{project}/locations/{region}/services/{id})"
        )
        private Property<String> metastoreService;

        @Schema(
            title = "Spark History Server",
            description = "Optional Dataproc cluster to serve Spark History for the workload"
        )
        @PluginProperty(dynamic = true)
        private SparkHistoryServerConfiguration sparkHistoryServer;
    }

    @Builder
    @Getter
    public static class SparkHistoryServerConfiguration {
        @Schema(
            title = "Spark History Server cluster",
            description = "Dataproc cluster resource name (projects/{project}/regions/{region}/clusters/{name})"
        )
        private Property<String> dataprocCluster;
    }

    @Builder
    @Getter
    public static class RuntimeConfiguration {
        @Schema(
            title = "Container image",
            description = "Optional custom runtime image; defaults to Dataproc image when unset"
        )
        private Property<String> containerImage;

        @Schema(
            title = "Runtime version",
            description = "Optional Dataproc runtime version string"
        )
        private Property<String> version;

        @Schema(
            title = "Runtime properties",
            description = "Key/value pairs passed to the runtime"
        )
        private Property<Map<String, String>> properties;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Batch state"
        )
        private final com.google.cloud.dataproc.v1.Batch.State state;
    }
}
