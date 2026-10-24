package io.kestra.plugin.gcp.gcs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.commons.io.FilenameUtils;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
class GcsTestUtils {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    Upload.Output upload(String out) throws Exception {
        return this.upload(out, "application.yml");
    }

    Upload.Output upload(String out, String resource) throws Exception {
        URI source;
        try (
            FileInputStream input = new FileInputStream(
                new File(
                    Objects.requireNonNull(
                        UploadTest.class.getClassLoader()
                            .getResource(resource)
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

        try (GcsTestConnections.Scope connections = GcsTestConnections.useFloci()) {
            Upload task = connections.spy(
                Upload.builder()
                    .id(UploadTest.class.getSimpleName())
                    .type(Upload.class.getName())
                    .from(Property.ofValue(source.toString()))
                    .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + "." + FilenameUtils.getExtension(resource)))
                    .build()
            );

            return task.run(runContext(task));
        } finally {
            storageInterface.delete(TenantService.MAIN_TENANT, null, source);
        }
    }

    Upload.Output update(String out) throws Exception {
        String content = "updated file: " + IdUtils.create();

        URI source;
        try (InputStream input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            source = storageInterface.put(
                TenantService.MAIN_TENANT,
                null,
                new URI("/" + FriendlyId.createFriendlyId()),
                input
            );
        }

        try (GcsTestConnections.Scope connections = GcsTestConnections.useFloci()) {
            Upload task = connections.spy(
                Upload.builder()
                    .id(UploadTest.class.getSimpleName())
                    .type(Upload.class.getName())
                    .from(Property.ofValue(source.toString()))
                    .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + ".yml"))
                    .build()
            );

            return task.run(runContext(task));
        } finally {
            storageInterface.delete(TenantService.MAIN_TENANT, null, source);
        }
    }

    RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );
    }
}
