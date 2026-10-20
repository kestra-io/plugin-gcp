package io.kestra.plugin.gcp.gcs;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
class UploadChecksumTest extends AbstractFlociGcpTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void defaultValidationPopulatesOutputChecksums() throws Exception {
        byte[] content = ("kestra-" + FriendlyId.createFriendlyId()).getBytes(StandardCharsets.UTF_8);

        try (
            AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint());
            UploadFixture fixture = uploadOf(content)
        ) {
            Upload task = fixture.task.build();
            Upload.Output run = task.run(mockRunContext(task));

            assertThat(run.getUri(), notNullValue());
            assertThat(run.getMd5(), equalTo(md5Base64(content)));
            assertThat(run.getCrc32c(), equalTo(crc32cBase64(content)));
            assertUploadedContents(fixture, content);
        }
    }

    @Test
    void disablingValidationSkipsChecksumComputation() throws Exception {
        byte[] content = "no-checksum".getBytes(StandardCharsets.UTF_8);

        try (
            AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint());
            UploadFixture fixture = uploadOf(content)
        ) {
            Upload task = fixture.task
                .validateChecksum(Property.ofValue(false))
                .build();

            Upload.Output run = task.run(mockRunContext(task));

            assertThat(run.getUri(), notNullValue());
            assertThat(run.getMd5(), nullValue());
            assertThat(run.getCrc32c(), nullValue());
            assertUploadedContents(fixture, content);
        }
    }

    @Test
    void expectedMd5StillEnforcedWhenValidationDisabled() throws Exception {
        byte[] content = "explicit-assertion".getBytes(StandardCharsets.UTF_8);

        try (
            AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint());
            UploadFixture fixture = uploadOf(content)
        ) {
            Upload task = fixture.task
                .validateChecksum(Property.ofValue(false))
                .expectedMd5(Property.ofValue(md5Base64(content)))
                .build();

            Upload.Output run = task.run(mockRunContext(task));

            assertThat(run.getMd5(), equalTo(md5Base64(content)));
            assertUploadedContents(fixture, content);
        }
    }

    @Test
    void expectedMd5MismatchThrowsBeforeServerCheck() throws Exception {
        byte[] content = "mismatch".getBytes(StandardCharsets.UTF_8);

        try (
            AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint());
            UploadFixture fixture = uploadOf(content)
        ) {
            Upload task = fixture.task
                .expectedMd5(Property.ofValue("AAAAAAAAAAAAAAAAAAAAAA=="))
                .build();

            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> task.run(mockRunContext(task)));
            assertThat(ex.getMessage(), containsString("Source MD5 mismatch"));
            assertUploadedContents(fixture, content);
        }
    }

    private UploadFixture uploadOf(byte[] content) throws Exception {
        URI source;
        try (ByteArrayInputStream input = new ByteArrayInputStream(content)) {
            source = storageInterface.put(
                TenantService.MAIN_TENANT,
                null,
                new URI("/" + FriendlyId.createFriendlyId()),
                input
            );
        }

        String objectName = "tasks/gcp/upload-checksum/" + FriendlyId.createFriendlyId() + ".bin";
        Upload.UploadBuilder<?, ?> task = Upload.builder()
            .id(UploadChecksumTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue("gs://{{inputs.bucket}}/" + objectName));

        return new UploadFixture(source, objectName, content.clone(), task);
    }

    private void assertUploadedContents(UploadFixture fixture, byte[] expectedContents) {
        Blob uploaded = storage.get(bucket, fixture.objectName);
        assertThat(uploaded, notNullValue());
        assertArrayEquals(expectedContents, uploaded.getContent());
    }

    private RunContext mockRunContext(Upload task) {
        return TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("bucket", bucket)
        );
    }

    private static String md5Base64(byte[] content) throws Exception {
        return Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content));
    }

    private static String crc32cBase64(byte[] content) {
        CRC32C crc = new CRC32C();
        crc.update(content);
        return Base64.getEncoder().encodeToString(
            ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt((int) crc.getValue()).array()
        );
    }

    private final class UploadFixture implements AutoCloseable {
        private final URI source;
        private final String objectName;
        private final byte[] content;
        private final Upload.UploadBuilder<?, ?> task;

        private UploadFixture(URI source, String objectName, byte[] content, Upload.UploadBuilder<?, ?> task) {
            this.source = source;
            this.objectName = objectName;
            this.content = content;
            this.task = task;
        }

        @Override
        public void close() throws Exception {
            storage.delete(bucket, objectName);
            storageInterface.delete(TenantService.MAIN_TENANT, null, source);
        }
    }
}
