package io.kestra.plugin.gcp.gcs;

import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@KestraTest
@Testcontainers
class UploadTest extends AbstractFlociGcpTest {
    @Inject
    private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        String out = FriendlyId.createFriendlyId();
        String objectName = "tasks/gcp/upload/" + out + ".yml";
        byte[] expectedContents;
        try (
            InputStream input = Objects.requireNonNull(
                UploadTest.class.getClassLoader().getResourceAsStream("application.yml")
            )
        ) {
            expectedContents = input.readAllBytes();
        }

        try (AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint())) {
            Upload.Output run = testUtils.upload(out);

            assertThat(run.getUri(), is(new URI("gs://" + bucket + "/" + objectName)));

            Blob uploaded = storage.get(bucket, objectName);
            assertThat(uploaded, notNullValue());
            assertArrayEquals(expectedContents, uploaded.getContent());
        } finally {
            storage.delete(bucket, objectName);
        }
    }
}
