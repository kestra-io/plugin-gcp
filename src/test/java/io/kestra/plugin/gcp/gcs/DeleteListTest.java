package io.kestra.plugin.gcp.gcs;

import org.junit.jupiter.api.Test;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@KestraTest
class DeleteListTest extends AbstractFlociGcpTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void run() throws Exception {
        String dir = FriendlyId.createFriendlyId();
        String prefix = "tasks/gcp/" + dir + "/";

        try (GcsTestConnections.Scope connections = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            try {
                for (int i = 0; i < 10; i++) {
                    ListTest.upload(storageInterface, bucket, runContextFactory, "/tasks/gcp/" + dir);
                }

                DeleteList task = connections.spy(
                    DeleteList.builder()
                        .id(DeleteList.class.getSimpleName())
                        .type(DeleteList.class.getName())
                        .concurrent(8)
                        .from(Property.ofValue("gs://" + this.bucket + "/" + prefix))
                        .build()
                );
                DeleteList.Output run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));

                assertThat(run.getCount(), is(10L));
                assertThat(run.getSize(), greaterThan(6000L));
                assertThat(
                    storage.list(bucket, Storage.BlobListOption.prefix(prefix))
                        .iterateAll()
                        .iterator()
                        .hasNext(),
                    is(false)
                );
            } finally {
                storage.list(bucket, Storage.BlobListOption.prefix(prefix))
                    .iterateAll()
                    .forEach(blob -> storage.delete(blob.getBlobId()));
            }
        }
    }
}
