package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
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
    title = "Stop a Compute Engine instance",
    description = "Stops a running Compute Engine instance and optionally waits for it to reach the `TERMINATED` state. The instance's disks and configuration are preserved."
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
public class Stop extends AbstractComputeTask implements RunnableTask<AbstractComputeTask.Output> {

    @Builder.Default
    @Schema(
        title = "Whether to wait for the instance to reach the `TERMINATED` state before completing the task",
        description = "Default: `true`."
    )
    @PluginProperty(group = "execution")
    private Property<Boolean> waitUntilStopped = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        this.credentials(runContext);
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rZone = runContext.render(this.zone).as(String.class).orElseThrow();
        var rInstanceName = runContext.render(this.instanceName).as(String.class).orElseThrow();
        var rWaitUntilStopped = runContext.render(this.waitUntilStopped).as(Boolean.class).orElse(true);
        var rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(DEFAULT_TIMEOUT);

        logger.info("Stopping Compute Engine instance '{}' in zone '{}'", rInstanceName, rZone);

        try (var client = this.instancesClient(runContext)) {
            var operationFuture = client.stopAsync(rProjectId, rZone, rInstanceName);

            if (!rWaitUntilStopped) {
                // not waiting, so skip the GET (it'd just show the old status).
                return AbstractComputeTask.Output.builder()
                    .instanceName(rInstanceName)
                    .status("STOPPING")
                    .build();
            }

            this.awaitOperation(operationFuture, rTimeout, rInstanceName, "stop");

            var instance = client.get(rProjectId, rZone, rInstanceName);
            logger.info("Compute Engine instance '{}' is now '{}'", rInstanceName, instance.getStatus());

            return this.instanceOutput(instance);
        }
    }
}
