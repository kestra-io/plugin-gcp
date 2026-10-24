package io.kestra.plugin.gcp.gcs;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.gcs.Upload.Output;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@KestraTest
class DownloadsTest extends AbstractFlociGcpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Inject
    private GcsTestUtils testUtils;

    @Inject
    private StorageInterface storageInterface;

    private final String random = IdUtils.create();

    @Test
    void run() throws Exception {
        String prefix = "tasks/gcp/upload/" + random + "/";
        List<URI> downloadedFiles = new ArrayList<>();

        try (
            GcsTestConnections.Scope connections = GcsTestConnections.useFloci(
                FLOCI_GCP.getEndpoint(),
                io.kestra.plugin.gcp.gcs.List.class,
                Delete.class
            )
        ) {
            byte[] expectedContents;
            try (
                InputStream input = Objects.requireNonNull(
                    DownloadsTest.class.getClassLoader().getResourceAsStream("application.yml")
                )
            ) {
                expectedContents = input.readAllBytes();
            }

            String out1 = FriendlyId.createFriendlyId();
            Output output = testUtils.upload(random + "/" + out1 + " (1).yml");
            String out2 = FriendlyId.createFriendlyId();
            testUtils.upload(random + "/" + out2);

            assertThat(countObjects(prefix), is(2));

            Downloads task = connections.spy(
                Downloads.builder()
                    .id(DownloadTest.class.getSimpleName())
                    .type(Downloads.class.getName())
                    .from(Property.ofValue("gs://" + bucket + "/" + prefix))
                    .action(Property.ofValue(ActionInterface.Action.DELETE))
                    .build()
            );

            Downloads.Output run = task.run(runContext(task));
            downloadedFiles.addAll(run.getOutputFiles().values());

            assertThat(output.getUri().toString(), run.getBlobs().size(), is(2));
            assertThat(run.getOutputFiles().size(), is(2));

            for (URI downloadedFile : downloadedFiles) {
                try (InputStream actualContents = storageInterface.get(TenantService.MAIN_TENANT, null, downloadedFile)) {
                    assertArrayEquals(expectedContents, actualContents.readAllBytes());
                }
            }

            assertThat(countObjects(prefix), is(0));
        } finally {
            try {
                for (URI downloadedFile : downloadedFiles) {
                    storageInterface.delete(TenantService.MAIN_TENANT, null, downloadedFile);
                }
            } finally {
                deleteObjects(prefix);
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

    private void deleteObjects(String prefix) {
        storage.list(bucket, Storage.BlobListOption.prefix(prefix))
            .iterateAll()
            .forEach(blob -> storage.delete(blob.getBlobId()));
    }

    private int countObjects(String prefix) {
        int count = 0;
        for (var blob : storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
            count++;
        }
        return count;
    }
}
