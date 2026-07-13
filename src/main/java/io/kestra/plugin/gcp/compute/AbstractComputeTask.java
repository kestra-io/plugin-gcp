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

    protected static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);

    // how long to wait for the kill request to land before we close the client
    private static final Duration KILL_ACK_TIMEOUT = Duration.ofSeconds(30);

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
        description = "Default: `PT10M`. Hitting this timeout fails the task but does not cancel the in-flight GCP operation."
    )
    @PluginProperty(group = "advanced")
    protected Property<Duration> timeout = Property.ofValue(DEFAULT_TIMEOUT);

    // what to run if we're killed mid-op. fires once.
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

    // stash the kill action. if we're already killed, run it now.
    protected void onKill(Runnable action) {
        this.killable.set(action);
        if (this.isKilled.get()) {
            action.run();
        }
    }

    // kill() comes from RunnableTask, which only the subclasses implement, so this covers them all.
    public void kill() {
        if (this.isKilled.compareAndSet(false, true)) {
            Optional.ofNullable(this.killable.get()).ifPresent(Runnable::run);
        }
    }

    // wait for the op, and turn SDK errors into something readable.
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
            // wait for the request to land, close() below won't flush it for us.
            client.stopAsync(projectId, zone, instanceName).getInitialFuture().get(KILL_ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            runContext.logger().warn("Could not stop instance '{}' on kill: {}", instanceName, e.getMessage());
        }
    }

    protected void safelyDelete(RunContext runContext, String projectId, String zone, String instanceName) {
        try (var client = this.instancesClient(runContext)) {
            runContext.logger().warn("Task killed, deleting instance '{}' to stop billing", instanceName);
            // wait for the request to land, close() below won't flush it for us.
            client.deleteAsync(projectId, zone, instanceName).getInitialFuture().get(KILL_ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            runContext.logger().warn("Could not delete instance '{}' on kill: {}", instanceName, e.getMessage());
        }
    }

    // compute ops don't throw on failure, they hand back an operation with the error inside.
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

        @Schema(
            title = "The instance status after the operation",
            description = "e.g. `PROVISIONING`, `STAGING`, `RUNNING`, `STOPPING`, `TERMINATED`."
        )
        private String status;

        @Schema(
            title = "The instance's external (public) IP address, if any",
            description = "Typically `null` once an instance is stopped, since ephemeral external IPs are released on stop."
        )
        private String externalIp;

        @Schema(title = "The instance's internal (private) IP address")
        private String internalIp;
    }
}
