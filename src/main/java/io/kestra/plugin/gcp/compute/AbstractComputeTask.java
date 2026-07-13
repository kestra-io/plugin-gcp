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

    // Compensating action to run if the task is killed while a remote operation is in flight (e.g. delete a VM
    // that is still being created so it does not keep billing), and a guard so kill() runs the action only once.
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

    // Registers the action kill() should run to compensate for an in-flight operation. If the task was already
    // killed before the operation was submitted, the action runs immediately.
    protected void onKill(Runnable action) {
        this.killable.set(action);
        if (this.isKilled.get()) {
            action.run();
        }
    }

    // Not annotated @Override: kill() is declared on RunnableTask, which only the concrete subclasses implement.
    // This inherited implementation satisfies that contract for all of them.
    public void kill() {
        if (this.isKilled.compareAndSet(false, true)) {
            Optional.ofNullable(this.killable.get()).ifPresent(Runnable::run);
        }
    }

    // Waits for a Compute Engine operation to complete within the timeout and surfaces failures as a clear,
    // actionable message rather than a raw SDK exception (e.g. instance already exists, or was not found).
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

    // Best-effort compensating action: stop an instance that is being started, ignoring failures.
    protected void safelyStop(RunContext runContext, String projectId, String zone, String instanceName) {
        try (var client = this.instancesClient(runContext)) {
            runContext.logger().warn("Task killed, stopping instance '{}' to stop billing", instanceName);
            client.stopAsync(projectId, zone, instanceName);
        } catch (Exception e) {
            runContext.logger().warn("Could not stop instance '{}' on kill: {}", instanceName, e.getMessage());
        }
    }

    // Best-effort compensating action: delete an instance that is being created, ignoring failures.
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
}
