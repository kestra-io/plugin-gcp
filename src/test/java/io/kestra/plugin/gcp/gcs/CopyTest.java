package io.kestra.plugin.gcp.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers
class CopyTest extends AbstractFlociGcpTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void run() throws Exception {
        String in = FriendlyId.createFriendlyId();
        String out = FriendlyId.createFriendlyId();
        String inputObject = "tasks/gcp/copy/" + in + ".yml";
        String outputObject = "tasks/gcp/copy/" + out + ".yml";
        URI source = null;
        byte[] expectedContents;
        try (
            InputStream input = Objects.requireNonNull(
                CopyTest.class.getClassLoader().getResourceAsStream("application.yml")
            )
        ) {
            expectedContents = input.readAllBytes();
        }

        try (AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint())) {
            try (
                FileInputStream input = new FileInputStream(
                    new File(
                        Objects.requireNonNull(
                            UploadTest.class.getClassLoader()
                                .getResource("application.yml")
                        )
                            .toURI()
                    )
                )
            ) {
                source = storageInterface.put(
                    TenantService.MAIN_TENANT,
                    null,
                    new URI("/" + FriendlyId.createFriendlyId()),
                    input
                );
            }

            Upload upload = Upload.builder()
                .id(CopyTest.class.getSimpleName())
                .type(Upload.class.getName())
                .from(Property.ofValue(source.toString()))
                .to(Property.ofValue("gs://{{inputs.bucket}}/" + inputObject))
                .build();

            upload.run(runContext(upload));

            Copy task = Copy.builder()
                .id(CopyTest.class.getSimpleName())
                .type(Copy.class.getName())
                .from(Property.ofValue("gs://{{inputs.bucket}}/" + inputObject))
                .to(Property.ofValue("gs://{{inputs.bucket}}/" + outputObject))
                .build();

            Copy.Output run = task.run(runContext(task));

            assertThat(run.getUri(), is(new URI("gs://" + bucket + "/" + outputObject)));

            Blob copied = storage.get(bucket, outputObject);
            assertThat(copied, notNullValue());
            assertArrayEquals(expectedContents, copied.getContent());
        } finally {
            storage.delete(bucket, inputObject);
            storage.delete(bucket, outputObject);
            if (source != null) {
                storageInterface.delete(TenantService.MAIN_TENANT, null, source);
            }
        }
    }

    @Test
    void sameException() throws Exception {
        String in = FriendlyId.createFriendlyId();

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .build();

        try (AutoCloseable ignored = AbstractGcs.useEmulatorEndpoint(FLOCI_GCP.getEndpoint())) {
            assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
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
