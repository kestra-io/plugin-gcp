package io.kestra.plugin.gcp.bigtable;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.ManagedChannelBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.CredentialService;
import io.kestra.plugin.gcp.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Shared base class for all Bigtable tasks: implements GcpInterface (projectId, serviceAccount,
 * impersonatedServiceAccount, scopes) confirmed against the real
 * io.kestra.plugin.gcp.GcpInterface / CredentialService, adds the Bigtable-specific instanceId
 * and an emulatorHost escape hatch for local testing, and builds the Bigtable data/admin clients.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBigtable extends Task implements GcpInterface {

    @NotNull
    @Schema(title = "The GCP project ID.")
    @PluginProperty(group = "connection")
    protected Property<String> projectId;

    @Schema(title = "The GCP service account.")
    @PluginProperty(secret = true, group = "execution")
    protected Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate.")
    @PluginProperty(secret = true, group = "advanced")
    protected Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used.")
    @PluginProperty(group = "advanced")
    protected Property<List<String>> scopes;

    @NotNull
    @Schema(
        title = "The Bigtable instance ID.",
        description = "An instance is a container for your tables within a given GCP project."
    )
    @PluginProperty(group = "connection")
    protected Property<String> instanceId;

    @Schema(
        title = "Bigtable emulator host (`host:port`), for local testing only.",
        description = "When set, the task connects to a local Bigtable emulator instead of the real " +
            "Bigtable service, using plaintext transport and no credentials. Not intended for production use."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> emulatorHost;

    /**
     * Builds a BigtableDataClient for row-level read/write/delete operations.
     * Caller is responsible for closing the returned client.
     */
    protected BigtableDataClient dataClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        String rProjectId = runContext.render(this.projectId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        String rInstanceId = runContext.render(this.instanceId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required instanceId"));
        Optional<String> rEmulatorHost = runContext.render(this.emulatorHost).as(String.class);

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder()
            .setProjectId(rProjectId)
            .setInstanceId(rInstanceId);

        if (rEmulatorHost.isPresent()) {
            settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
            settingsBuilder.stubSettings()
                .setEndpoint(rEmulatorHost.get())
                .setTransportChannelProvider(emulatorTransportChannelProvider(rEmulatorHost.get()));
        } else {
            settingsBuilder.setCredentialsProvider(
                FixedCredentialsProvider.create(CredentialService.credentials(runContext, this))
            );
        }

        return BigtableDataClient.create(settingsBuilder.build());
    }

    /**
     * Builds a BigtableTableAdminClient for table-level operations (create/delete table).
     * Caller is responsible for closing the returned client.
     */
    protected BigtableTableAdminClient adminClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        String rProjectId = runContext.render(this.projectId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required projectId"));
        String rInstanceId = runContext.render(this.instanceId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("Missing required instanceId"));
        Optional<String> rEmulatorHost = runContext.render(this.emulatorHost).as(String.class);

        BigtableTableAdminSettings.Builder settingsBuilder = BigtableTableAdminSettings.newBuilder()
            .setProjectId(rProjectId)
            .setInstanceId(rInstanceId);

        if (rEmulatorHost.isPresent()) {
            settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
            settingsBuilder.stubSettings()
                .setEndpoint(rEmulatorHost.get())
                .setTransportChannelProvider(emulatorTransportChannelProvider(rEmulatorHost.get()));
        } else {
            settingsBuilder.setCredentialsProvider(
                FixedCredentialsProvider.create(CredentialService.credentials(runContext, this))
            );
        }

        return BigtableTableAdminClient.create(settingsBuilder.build());
    }

    /**
     * Plaintext (no TLS), no-auth gRPC transport channel provider pointed at a local Bigtable
     * emulator host:port. Built directly via InstantiatingGrpcChannelProvider rather than any
     * settings-class-specific "default emulator transport" static helper, since that helper's
     * exact name/location varies across google-cloud-bigtable versions and isn't worth
     * depending on.
     */
    private static com.google.api.gax.rpc.TransportChannelProvider emulatorTransportChannelProvider(String emulatorHost) {
        return com.google.api.gax.grpc.InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint(emulatorHost)
            .setChannelConfigurator(builder -> ((ManagedChannelBuilder<?>) builder).usePlaintext())
            .build();
    }
}

