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
    title = "Start a Compute Engine instance",
    description = "Starts an existing, stopped Compute Engine instance and optionally waits for it to reach the `RUNNING` state."
)
@Plugin(
    examples = {
        @Example(
            title = "Start an existing instance and log its IP.",
            full = true,
            code = """
                id: compute_engine_start
                namespace: company.team

                tasks:
                  - id: start_instance
                    type: io.kestra.plugin.gcp.compute.Start
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    zone: "europe-west1-b"
                    instanceName: "my-batch-vm"
                    waitUntilRunning: "true"

                  - id: log_status
                    type: io.kestra.plugin.core.log.Log
                    message: "Instance {{ outputs.start_instance.instanceName }} is {{ outputs.start_instance.status }} at {{ outputs.start_instance.externalIp }}"
                """
        )
    }
)
public class Start extends AbstractComputeTask implements RunnableTask<Start.Output> {

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

        this.credentials(runContext);
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rZone = runContext.render(this.zone).as(String.class).orElseThrow();
        var rInstanceName = runContext.render(this.instanceName).as(String.class).orElseThrow();
        var rWaitUntilRunning = runContext.render(this.waitUntilRunning).as(Boolean.class).orElse(true);
        var rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(Duration.ofMinutes(10));

        logger.info("Starting Compute Engine instance '{}' in zone '{}'", rInstanceName, rZone);

        try (var client = this.instancesClient(runContext)) {
            var operationFuture = client.startAsync(rProjectId, rZone, rInstanceName);

            // If the task is killed while the VM is starting, stop it again so it does not keep billing.
            this.onKill(() -> this.safelyStop(runContext, rProjectId, rZone, rInstanceName));

            if (rWaitUntilRunning) {
                this.awaitOperation(operationFuture, rTimeout, rInstanceName, "start");
            }

            var instance = client.get(rProjectId, rZone, rInstanceName);
            logger.info("Compute Engine instance '{}' is now '{}'", rInstanceName, instance.getStatus());

            return Output.builder()
                .instanceName(instance.getName())
                .instanceId(String.valueOf(instance.getId()))
                .status(instance.getStatus())
                .externalIp(externalIp(instance))
                .internalIp(internalIp(instance))
                .build();
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The name of the started instance"
        )
        private String instanceName;

        @Schema(
            title = "The unique GCP instance ID"
        )
        private String instanceId;

        @Schema(
            title = "The instance status after the operation",
            description = "e.g. `RUNNING`, `STAGING`."
        )
        private String status;

        @Schema(
            title = "The instance's external (public) IP address, if any"
        )
        private String externalIp;

        @Schema(
            title = "The instance's internal (private) IP address"
        )
        private String internalIp;
    }
}
