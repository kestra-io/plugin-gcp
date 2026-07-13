package io.kestra.plugin.gcp.compute;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.compute.v1.AccessConfig;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.InstancesSettings;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.Operation;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
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
public abstract class AbstractComputeTask extends AbstractTask {

    @NotNull
    @Schema(
        title = "The zone where the instance is located",
        description = "e.g. `us-central1-a`."
    )
    @PluginProperty(group = "main")
    protected Property<String> zone;

    @NotNull
    @Schema(
        title = "The name of the Compute Engine instance"
    )
    @PluginProperty(group = "main")
    protected Property<String> instanceName;

    @Builder.Default
    @Schema(
        title = "The maximum duration to wait for the operation to complete",
        description = "Default: `PT10M`."
    )
    @PluginProperty(group = "advanced")
    protected Property<Duration> timeout = Property.ofValue(Duration.ofMinutes(10));

    // Compensating action run once if the task is killed while a remote operation is in flight.
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    protected InstancesClient instancesClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        var settings = InstancesSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
            .build();

        return InstancesClient.create(settings);
    }

    // Registers the compensating action for kill(); runs it immediately if the task was already killed.
    protected void onKill(Runnable action) {
        this.killable.set(action);
        if (this.isKilled.get()) {
            action.run();
        }
    }

    // kill() is declared on RunnableTask (implemented by the concrete subclasses); this satisfies it for all of them.
    public void kill() {
        if (this.isKilled.compareAndSet(false, true)) {
            Optional.ofNullable(this.killable.get()).ifPresent(Runnable::run);
        }
    }

    // Waits for the operation within the timeout, translating SDK failures into an actionable message.
    protected void awaitOperation(OperationFuture<Operation, Operation> operationFuture, Duration timeout, String instanceName, String action) throws Exception {
        Operation operation;
        try {
            operation = operationFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out after " + timeout + " waiting to " + action + " instance '" + instanceName + "'.");
        } catch (ExecutionException e) {
            var cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException("Failed to " + action + " instance '" + instanceName + "': " + cause.getMessage(), cause);
        }

        checkOperationError(operation);
    }

    protected void safelyStop(RunContext runContext, String projectId, String zone, String instanceName) {
        try (var client = this.instancesClient(runContext)) {
            runContext.logger().warn("Task killed, stopping instance '{}' to stop billing", instanceName);
            client.stopAsync(projectId, zone, instanceName);
        } catch (Exception e) {
            runContext.logger().warn("Could not stop instance '{}' on kill: {}", instanceName, e.getMessage());
        }
    }

    protected void safelyDelete(RunContext runContext, String projectId, String zone, String instanceName) {
        try (var client = this.instancesClient(runContext)) {
            runContext.logger().warn("Task killed, deleting instance '{}' to stop billing", instanceName);
            client.deleteAsync(projectId, zone, instanceName);
        } catch (Exception e) {
            runContext.logger().warn("Could not delete instance '{}' on kill: {}", instanceName, e.getMessage());
        }
    }

    // Compute Engine's long-running operations carry their errors on the Operation itself rather than
    // throwing: an OperationFuture can complete successfully while wrapping a failed operation.
    protected static void checkOperationError(Operation operation) {
        if (operation.hasError()) {
            var message = operation.getError().getErrorsList().stream()
                .map(error -> error.getCode() + ": " + error.getMessage())
                .collect(Collectors.joining("; "));

            throw new IllegalStateException("Compute Engine operation failed: " + message);
        }
    }

    protected static String externalIp(Instance instance) {
        return instance.getNetworkInterfacesList().stream()
            .flatMap(networkInterface -> networkInterface.getAccessConfigsList().stream())
            .map(AccessConfig::getNatIP)
            .filter(ip -> ip != null && !ip.isEmpty())
            .findFirst()
            .orElse(null);
    }

    protected static String internalIp(Instance instance) {
        return instance.getNetworkInterfacesList().stream()
            .map(NetworkInterface::getNetworkIP)
            .filter(ip -> ip != null && !ip.isEmpty())
            .findFirst()
            .orElse(null);
    }

    protected Output instanceOutput(Instance instance) {
        return Output.builder()
            .instanceName(instance.getName())
            .instanceId(String.valueOf(instance.getId()))
            .status(instance.getStatus())
            .externalIp(externalIp(instance))
            .internalIp(internalIp(instance))
            .build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The name of the instance")
        private String instanceName;

        @Schema(title = "The unique GCP instance ID")
        private String instanceId;

        @Schema(title = "The instance status after the operation")
        private String status;

        @Schema(title = "The instance's external (public) IP address, if any")
        private String externalIp;

        @Schema(title = "The instance's internal (private) IP address")
        private String internalIp;
    }
}
