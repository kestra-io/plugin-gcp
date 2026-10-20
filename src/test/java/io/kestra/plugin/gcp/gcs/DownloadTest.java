package io.kestra.plugin.gcp.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@KestraTest
@Testcontainers
class DownloadTest extends AbstractFlociGcpTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        String out = FriendlyId.createFriendlyId();
        String objectName = "tasks/gcp/upload/" + out + ".yml";
        URI source = null;
        URI downloaded = null;

        try (AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint())) {
            File file = new File(
                Objects.requireNonNull(
                    DownloadTest.class.getClassLoader()
                        .getResource("application.yml")
                )
                    .toURI()
            );

            try (InputStream input = new FileInputStream(file)) {
                source = storageInterface.put(
                    TenantService.MAIN_TENANT,
                    null,
                    new URI("/" + FriendlyId.createFriendlyId()),
                    input
                );
            }

            Upload upload = Upload.builder()
                .id(UploadTest.class.getSimpleName())
                .type(Upload.class.getName())
                .from(Property.ofValue(source.toString()))
                .to(Property.ofValue("gs://{{inputs.bucket}}/" + objectName))
                .build();

            Upload.Output uploadOutput = upload.run(runContext(upload));
            assertNotNull(storage.get(bucket, objectName));

            Download task = Download.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Download.class.getName())
                .from(Property.ofValue(uploadOutput.getUri().toString()))
                .build();

            Download.Output run = task.run(runContext(task));
            downloaded = run.getUri();
            assertThat(downloaded.toString(), endsWith(".yml"));

            try (
                InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, downloaded);
                InputStream expected = new FileInputStream(file)
            ) {
                assertThat(
                    CharStreams.toString(new InputStreamReader(get)),
                    is(CharStreams.toString(new InputStreamReader(expected)))
                );
            }
        } finally {
            storage.delete(bucket, objectName);
            if (source != null) {
                storageInterface.delete(TenantService.MAIN_TENANT, null, source);
            }
            if (downloaded != null) {
                storageInterface.delete(TenantService.MAIN_TENANT, null, downloaded);
            }
        }
    }

    @Test
    void fromStorageWithChecksumValidation() throws Exception {
        String out = FriendlyId.createFriendlyId();
        String objectName = "tasks/gcp/upload/" + out + ".yml";
        URI source = null;
        URI downloaded = null;

        try (AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint())) {
            File file = new File(
                Objects.requireNonNull(
                    DownloadTest.class.getClassLoader()
                        .getResource("application.yml")
                )
                    .toURI()
            );

            try (InputStream input = new FileInputStream(file)) {
                source = storageInterface.put(
                    TenantService.MAIN_TENANT,
                    null,
                    new URI("/" + FriendlyId.createFriendlyId()),
                    input
                );
            }

            Upload upload = Upload.builder()
                .id(UploadTest.class.getSimpleName())
                .type(Upload.class.getName())
                .from(Property.ofValue(source.toString()))
                .to(Property.ofValue("gs://{{inputs.bucket}}/" + objectName))
                .build();

            Upload.Output uploadOutput = upload.run(runContext(upload));
            assertNotNull(storage.get(bucket, objectName));

            Download task = Download.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Download.class.getName())
                .from(Property.ofValue(uploadOutput.getUri().toString()))
                .validateChecksum(Property.ofValue(true))
                .build();

            Download.Output run = task.run(runContext(task));
            downloaded = run.getUri();
            assertThat(downloaded.toString(), endsWith(".yml"));

            try (
                InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, downloaded);
                InputStream expected = new FileInputStream(file)
            ) {
                assertThat(
                    CharStreams.toString(new InputStreamReader(get)),
                    is(CharStreams.toString(new InputStreamReader(expected)))
                );
            }
        } finally {
            storage.delete(bucket, objectName);
            if (source != null) {
                storageInterface.delete(TenantService.MAIN_TENANT, null, source);
            }
            if (downloaded != null) {
                storageInterface.delete(TenantService.MAIN_TENANT, null, downloaded);
            }
        }
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );
    }
}
