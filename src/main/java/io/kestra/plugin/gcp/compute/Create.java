package io.kestra.plugin.gcp.compute;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.compute.v1.AccessConfig;
import com.google.cloud.compute.v1.AttachedDisk;
import com.google.cloud.compute.v1.AttachedDiskInitializeParams;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.Items;
import com.google.cloud.compute.v1.Metadata;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.Scheduling;
import com.google.cloud.compute.v1.Tags;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a Compute Engine instance",
    description = "Creates a new virtual machine instance on Google Compute Engine, with an optional startup script, network tags, and custom metadata. " +
        "By default, an ephemeral external IP is attached to the instance's default network interface."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a VM instance and then stop it.",
            full = true,
            code = """
                id: compute_engine_create
                namespace: company.team

                tasks:
                  - id: create_instance
                    type: io.kestra.plugin.gcp.compute.Create
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    zone: "us-central1-a"
                    instanceName: "kestra-job-{{ execution.id }}"
                    machineType: "n1-standard-2"
                    sourceImage: "projects/debian-cloud/global/images/family/debian-11"
                    startupScript: "apt-get update && apt-get install -y python3"
                    waitUntilRunning: "true"

                  - id: stop_instance
                    type: io.kestra.plugin.gcp.compute.Stop
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    zone: "us-central1-a"
                    instanceName: "{{ outputs.create_instance.instanceName }}"
                """
        )
    }
)
public class Create extends AbstractComputeTask implements RunnableTask<AbstractComputeTask.Output> {

    @NotNull
    @Schema(
        title = "The machine type to use for the instance",
        description = "e.g. `n1-standard-1`."
    )
    @PluginProperty(group = "main")
    private Property<String> machineType;

    @NotNull
    @Schema(
        title = "The boot disk source image",
        description = "e.g. `projects/debian-cloud/global/images/family/debian-11`."
    )
    @PluginProperty(group = "main")
    private Property<String> sourceImage;

    @Schema(
        title = "The network to attach the instance to",
        description = "e.g. `global/networks/my-network`. When neither `networkName` nor `subnetworkName` is set, " +
            "the project's `default` network is used. On projects that use a custom subnet-mode network, leave this " +
            "unset and set `subnetworkName` instead so the network is inferred from the subnetwork."
    )
    @PluginProperty(group = "advanced")
    private Property<String> networkName;

    @Schema(
        title = "The subnetwork to attach the instance to",
        description = "e.g. `regions/us-central1/subnetworks/my-subnetwork`. Required on projects whose network is in " +
            "custom subnet mode."
    )
    @PluginProperty(group = "advanced")
    private Property<String> subnetworkName;

    @Schema(
        title = "Network tags to apply to the instance"
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> tags;

    @Schema(
        title = "Custom metadata key-value pairs to attach to the instance"
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> metadata;

    @Schema(
        title = "A startup script to run when the instance boots",
        description = "Stored as the `startup-script` instance metadata entry."
    )
    @PluginProperty(group = "advanced")
    private Property<String> startupScript;

    @Schema(
        title = "The boot disk size, in GB",
        description = "Must be at least 10GB, the minimum GCP allows for most public images."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> diskSizeGb;

    @Schema(
        title = "The boot disk type",
        description = "e.g. `pd-standard`, `pd-ssd`."
    )
    @PluginProperty(group = "advanced")
    private Property<String> diskType;

    @Builder.Default
    @Schema(
        title = "Whether the instance is preemptible",
        description = "Default: `false`."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> preemptible = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Whether to wait for the instance to reach the `RUNNING` state before completing the task",
        description = "Default: `true`."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> waitUntilRunning = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        // credentials() must be called before rendering projectId: it infers the project ID from the
        // service account when the property is not explicitly set.
        this.credentials(runContext);
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rZone = runContext.render(this.zone).as(String.class).orElseThrow();
        var rInstanceName = runContext.render(this.instanceName).as(String.class).orElseThrow();
        var rWaitUntilRunning = runContext.render(this.waitUntilRunning).as(Boolean.class).orElse(true);
        var rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(DEFAULT_TIMEOUT);

        var instanceResource = this.buildInstanceResource(runContext, rZone, rInstanceName);

        logger.info("Creating Compute Engine instance '{}' in zone '{}'", rInstanceName, rZone);

        try (var client = this.instancesClient(runContext)) {
            var operationFuture = client.insertAsync(rProjectId, rZone, instanceResource);

            // On kill, delete the VM that is still being created so it does not keep billing.
            this.onKill(() -> this.safelyDelete(runContext, rProjectId, rZone, rInstanceName));

            if (!rWaitUntilRunning) {
                // Not waiting: the instance may not be queryable yet, so avoid racing a GET against the in-flight insert.
                return Output.builder()
                    .instanceName(rInstanceName)
                    .status("PROVISIONING")
                    .build();
            }

            this.awaitOperation(operationFuture, rTimeout, rInstanceName, "create");

            var instance = client.get(rProjectId, rZone, rInstanceName);
            logger.info("Compute Engine instance '{}' is now '{}'", rInstanceName, instance.getStatus());

            return this.instanceOutput(instance);
        }
    }

    Instance buildInstanceResource(RunContext runContext, String zone, String instanceName) throws IllegalVariableEvaluationException {
        var rMachineType = runContext.render(this.machineType).as(String.class).orElseThrow();
        var rSourceImage = runContext.render(this.sourceImage).as(String.class).orElseThrow();

        var bootDisk = AttachedDisk.newBuilder()
            .setBoot(true)
            .setAutoDelete(true)
            .setInitializeParams(this.buildDiskInitializeParams(runContext, zone, rSourceImage));

        var networkInterface = NetworkInterface.newBuilder()
            .addAccessConfigs(
                AccessConfig.newBuilder()
                    .setName("External NAT")
                    .setType("ONE_TO_ONE_NAT")
                    .build()
            );

        if (this.subnetworkName != null) {
            networkInterface.setSubnetwork(runContext.render(this.subnetworkName).as(String.class).orElseThrow());
        }

        // Only set the network explicitly when the user asked for one. When only a subnetwork is given, GCP infers
        // the network from it. When neither is set, fall back to the project's default network. Forcing the default
        // network alongside a custom-mode subnetwork would be rejected as a network/subnetwork mismatch.
        if (this.networkName != null) {
            networkInterface.setNetwork(runContext.render(this.networkName).as(String.class).orElseThrow());
        } else if (this.subnetworkName == null) {
            networkInterface.setNetwork("global/networks/default");
        }

        var instanceBuilder = Instance.newBuilder()
            .setName(instanceName)
            .setMachineType("zones/" + zone + "/machineTypes/" + rMachineType)
            .addDisks(bootDisk)
            .addNetworkInterfaces(networkInterface)
            .setScheduling(
                Scheduling.newBuilder()
                    .setPreemptible(runContext.render(this.preemptible).as(Boolean.class).orElse(false))
                    .build()
            );

        if (this.tags != null) {
            var rTags = runContext.render(this.tags).asList(String.class);
            if (!rTags.isEmpty()) {
                instanceBuilder.setTags(Tags.newBuilder().addAllItems(rTags).build());
            }
        }

        var metadataItems = new ArrayList<Items>();
        if (this.metadata != null) {
            runContext.render(this.metadata).asMap(String.class, String.class)
                .forEach((key, value) -> metadataItems.add(Items.newBuilder().setKey(key).setValue(value).build()));
        }
        if (this.startupScript != null) {
            var rStartupScript = runContext.render(this.startupScript).as(String.class).orElseThrow();
            metadataItems.add(Items.newBuilder().setKey("startup-script").setValue(rStartupScript).build());
        }
        if (!metadataItems.isEmpty()) {
            instanceBuilder.setMetadata(Metadata.newBuilder().addAllItems(metadataItems).build());
        }

        return instanceBuilder.build();
    }

    private AttachedDiskInitializeParams buildDiskInitializeParams(RunContext runContext, String zone, String sourceImage) throws IllegalVariableEvaluationException {
        var builder = AttachedDiskInitializeParams.newBuilder().setSourceImage(sourceImage);

        if (this.diskSizeGb != null) {
            builder.setDiskSizeGb(runContext.render(this.diskSizeGb).as(Integer.class).orElseThrow());
        }

        if (this.diskType != null) {
            builder.setDiskType("zones/" + zone + "/diskTypes/" + runContext.render(this.diskType).as(String.class).orElseThrow());
        }

        return builder.build();
    }
}
