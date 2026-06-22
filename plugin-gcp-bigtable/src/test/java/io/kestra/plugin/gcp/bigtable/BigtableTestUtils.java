package io.kestra.plugin.gcp.bigtable;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.DockerClientFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;

/**
 * Shared test fixtures backed by the official Bigtable emulator image.
 */
public abstract class BigtableTestUtils {

    protected static final String PROJECT_ID = "test-project";
    protected static final String INSTANCE_ID = "test-instance";

    protected static final GenericContainer<?> BIGTABLE_EMULATOR =
        new GenericContainer<>("google/cloud-sdk:emulators")
            .withCommand("gcloud", "beta", "emulators", "bigtable", "start", "--host-port=0.0.0.0:8086")
            .withExposedPorts(8086);

    protected static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable e) {
            return false;
        }
    }

    @BeforeAll
    static void setUpEmulator() {
        Assumptions.assumeTrue(isDockerAvailable(), "Docker is not available, skipping Bigtable tests");
        if (!BIGTABLE_EMULATOR.isRunning()) {
            BIGTABLE_EMULATOR.start();
        }
    }

    @AfterAll
    static void tearDownEmulator() {
        if (BIGTABLE_EMULATOR.isRunning()) {
            BIGTABLE_EMULATOR.stop();
        }
    }

    protected static String getEmulatorHost() {
        return BIGTABLE_EMULATOR.getHost() + ":" + BIGTABLE_EMULATOR.getMappedPort(8086);
    }

    protected static BigtableDataClient createDataClient() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(getEmulatorHost())
            .usePlaintext()
            .build();
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(
            GrpcTransportChannel.create(channel)
        );

        BigtableDataSettings.Builder settingsBuilder = BigtableDataSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID);
        settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        settingsBuilder.stubSettings().setTransportChannelProvider(channelProvider);

        return BigtableDataClient.create(settingsBuilder.build());
    }

    protected static BigtableTableAdminClient createAdminClient() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(getEmulatorHost())
            .usePlaintext()
            .build();
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(
            GrpcTransportChannel.create(channel)
        );

        BigtableTableAdminSettings.Builder settingsBuilder = BigtableTableAdminSettings.newBuilder()
            .setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID);
        settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        settingsBuilder.stubSettings().setTransportChannelProvider(channelProvider);

        return BigtableTableAdminClient.create(settingsBuilder.build());
    }

    protected static void createTestTable(String tableId, String columnFamily) throws Exception {
        try (BigtableTableAdminClient admin = createAdminClient()) {
            admin.createTable(CreateTableRequest.of(tableId).addFamily(columnFamily));
        }
    }
}