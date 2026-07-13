package io.kestra.plugin.gcp.compute;

import java.time.Duration;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.compute.v1.Instance;

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
    title = "Delete a Compute Engine instance",
    description = "Deletes a Compute Engine instance and, by default, waits for the deletion to complete. " +
        "Boot disks created with the instance are removed along with it; additional disks attached separately are kept unless they were created with auto-delete enabled."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete an instance when a flow completes.",
            full = true,
            code = """
                id: compute_engine_cleanup
                namespace: company.team

                triggers:
                  - id: on_flow_completion
                    type: io.kestra.plugin.core.trigger.Flow
                    conditions:
                      - type: io.kestra.plugin.core.condition.ExecutionStatusCondition
                        in:
                          - SUCCESS
                          - FAILED

                tasks:
                  - id: delete_instance
                    type: io.kestra.plugin.gcp.compute.Delete
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    zone: "us-central1-a"
                    instanceName: "kestra-job-{{ trigger.executionId }}"
                    waitUntilDeleted: "true"
                """
        )
    }
)
public class Delete extends AbstractComputeTask implements RunnableTask<Delete.Output> {

    @Builder.Default
    @Schema(
        title = "Whether to wait for the instance to be fully deleted before completing the task",
        description = "Default: `true`."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> waitUntilDeleted = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        this.credentials(runContext);
        var rProjectId = runContext.render(this.projectId).as(String.class).orElseThrow();
        var rZone = runContext.render(this.zone).as(String.class).orElseThrow();
        var rInstanceName = runContext.render(this.instanceName).as(String.class).orElseThrow();
        var rWaitUntilDeleted = runContext.render(this.waitUntilDeleted).as(Boolean.class).orElse(true);
        var rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(DEFAULT_TIMEOUT);

        // nothing new to bill for here, so no kill hook.
        try (var client = this.instancesClient(runContext)) {
            Instance existing;
            try {
                existing = client.get(rProjectId, rZone, rInstanceName);
            } catch (NotFoundException e) {
                // already gone, so call it done. keeps re-runs idempotent.
                logger.info("Instance '{}' was not found; treating it as already deleted.", rInstanceName);
                return Output.builder()
                    .instanceName(rInstanceName)
                    .status("DELETED")
                    .build();
            }

            logger.info("Deleting Compute Engine instance '{}' in zone '{}'", rInstanceName, rZone);
            var operationFuture = client.deleteAsync(rProjectId, rZone, rInstanceName);

            var status = "PENDING_DELETE";
            if (rWaitUntilDeleted) {
                this.awaitOperation(operationFuture, rTimeout, rInstanceName, "delete");
                status = "DELETED";
                logger.info("Compute Engine instance '{}' deleted", rInstanceName);
            }

            return Output.builder()
                .instanceName(rInstanceName)
                .instanceId(String.valueOf(existing.getId()))
                .status(status)
                .build();
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The name of the deleted instance"
        )
        private String instanceName;

        @Schema(
            title = "The unique GCP instance ID of the deleted instance, if it could be resolved before deletion"
        )
        private String instanceId;

        @Schema(
            title = "The instance status after the operation",
            description = "`DELETED` if the deletion completed, `PENDING_DELETE` if `waitUntilDeleted` is `false`."
        )
        private String status;
    }
}
