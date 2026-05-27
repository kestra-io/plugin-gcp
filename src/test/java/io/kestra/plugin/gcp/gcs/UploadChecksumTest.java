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
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class UploadChecksumTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void defaultValidationPopulatesOutputChecksums() throws Exception {
        byte[] content = ("kestra-" + FriendlyId.createFriendlyId()).getBytes(StandardCharsets.UTF_8);
        Upload task = uploadOf(content).build();

        Upload.Output run = task.run(mockRunContext(task));

        assertThat(run.getUri(), notNullValue());
        assertThat(run.getMd5(), equalTo(md5Base64(content)));
        assertThat(run.getCrc32c(), equalTo(crc32cBase64(content)));
    }

    @Test
    void disablingValidationSkipsChecksumComputation() throws Exception {
        byte[] content = "no-checksum".getBytes(StandardCharsets.UTF_8);
        Upload task = uploadOf(content)
            .validateChecksum(Property.ofValue(false))
            .build();

        Upload.Output run = task.run(mockRunContext(task));

        assertThat(run.getUri(), notNullValue());
        assertThat(run.getMd5(), nullValue());
        assertThat(run.getCrc32c(), nullValue());
    }

    @Test
    void expectedMd5StillEnforcedWhenValidationDisabled() throws Exception {
        byte[] content = "explicit-assertion".getBytes(StandardCharsets.UTF_8);
        Upload task = uploadOf(content)
            .validateChecksum(Property.ofValue(false))
            .expectedMd5(Property.ofValue(md5Base64(content)))
            .build();

        Upload.Output run = task.run(mockRunContext(task));

        assertThat(run.getMd5(), equalTo(md5Base64(content)));
    }

    @Test
    void expectedMd5MismatchThrowsBeforeServerCheck() throws Exception {
        byte[] content = "mismatch".getBytes(StandardCharsets.UTF_8);
        Upload task = uploadOf(content)
            .expectedMd5(Property.ofValue("AAAAAAAAAAAAAAAAAAAAAA=="))
            .build();
        RunContext rc = mockRunContext(task);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> task.run(rc));
        assertThat(ex.getMessage(), containsString("Source MD5 mismatch"));
    }

    private Upload.UploadBuilder<?, ?> uploadOf(byte[] content) throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new ByteArrayInputStream(content)
        );

        return Upload.builder()
            .id(UploadChecksumTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/upload-checksum/" + FriendlyId.createFriendlyId() + ".bin"));
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
}
