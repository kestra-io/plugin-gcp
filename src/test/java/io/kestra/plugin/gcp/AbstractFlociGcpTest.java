package io.kestra.plugin.gcp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.floci.testcontainers.gcp.FlociGcpContainer;

public abstract class AbstractFlociGcpTest {
    protected static final String PROJECT_ID = "kestra-unit-test";
    protected static final String BUCKET_NAME = "kestra-unit-test";

    @Container
    protected static final FlociGcpContainer FLOCI_GCP = new FlociGcpContainer("floci/floci-gcp:0.11.0")
        .withProjectId(PROJECT_ID);

    protected static Storage storage;

    @BeforeAll
    protected static void initializeGcs() {
        storage = StorageOptions.newBuilder()
            .setHost(FLOCI_GCP.getEndpoint())
            .setProjectId(PROJECT_ID)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getService();

        if (storage.get(BUCKET_NAME) == null) {
            storage.create(BucketInfo.of(BUCKET_NAME));
        }
    }

    @AfterAll
    protected static void closeGcsClient() throws Exception {
        if (storage != null) {
            storage.close();
            storage = null;
        }
    }
}
