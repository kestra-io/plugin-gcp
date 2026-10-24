package io.kestra.plugin.gcp.gcs;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;

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
import static org.hamcrest.Matchers.*;

@KestraTest
class ComposeTest extends AbstractFlociGcpTest {
    @Inject
    private GcsTestUtils testUtils;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void run() throws Exception {
        String dir = FriendlyId.createFriendlyId();
        String sourcePrefix = "tasks/gcp/upload/compose-" + dir + "/";
        String outputObject = "tasks/gcp/compose-result/" + dir + "/compose.txt";
        URI downloaded = null;

        try (
            GcsTestConnections.Scope connections = GcsTestConnections.useFloci(
                FLOCI_GCP.getEndpoint(),
                io.kestra.plugin.gcp.gcs.List.class
            )
        ) {
            testUtils.upload("compose-" + dir + "/compose1/" + FriendlyId.createFriendlyId(), "data/1.txt");
            testUtils.upload("compose-" + dir + "/compose2/" + FriendlyId.createFriendlyId(), "data/2.txt");
            testUtils.upload("compose-" + dir + "/compose3/" + FriendlyId.createFriendlyId(), "data/3.txt");

            Compose task = connections.spy(
                Compose.builder()
                    .id(ComposeTest.class.getSimpleName())
                    .type(Compose.class.getName())
                    .list(
                        Compose.List.builder()
                            .from(Property.ofValue("gs://" + bucket + "/" + sourcePrefix))
                            .listingType(Property.ofValue(ListInterface.ListingType.RECURSIVE))
                            .build()
                    )
                    .to(Property.ofValue("gs://" + bucket + "/" + outputObject))
                    .build()
            );

            RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());
            Compose.Output run = task.run(runContext);

            Blob composed = storage.get(bucket, outputObject);
            assertThat(composed, notNullValue());
            assertThat(new String(composed.getContent(), StandardCharsets.UTF_8), is("1\n2\n3\n"));

            Download download = connections.spy(
                Download.builder()
                    .id(DownloadTest.class.getSimpleName())
                    .type(Download.class.getName())
                    .from(Property.ofValue(run.getUri().toString()))
                    .build()
            );

            downloaded = download.run(runContext).getUri();
            try (InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, downloaded)) {
                assertThat(
                    CharStreams.toString(new InputStreamReader(get, StandardCharsets.UTF_8)),
                    is("1\n2\n3\n")
                );
            }
        } finally {
            try {
                if (downloaded != null) {
                    storageInterface.delete(TenantService.MAIN_TENANT, null, downloaded);
                }
            } finally {
                try {
                    deleteObjects(sourcePrefix);
                } finally {
                    storage.delete(bucket, outputObject);
                }
            }
        }
    }

    @Test
    void runWithDefaultListingType() throws Exception {
        String dir = FriendlyId.createFriendlyId();
        String sourcePrefix = "tasks/gcp/upload/compose-" + dir + "/";
        String basePath = "compose-" + dir + "/";
        String outputObject = "tasks/gcp/compose-result/" + dir + "/compose-default.txt";
        URI downloaded = null;

        try (
            GcsTestConnections.Scope connections = GcsTestConnections.useFloci(
                FLOCI_GCP.getEndpoint(),
                io.kestra.plugin.gcp.gcs.List.class
            )
        ) {
            testUtils.upload(basePath + FriendlyId.createFriendlyId(), "data/1.txt");
            testUtils.upload(basePath + FriendlyId.createFriendlyId(), "data/2.txt");
            testUtils.upload(basePath + FriendlyId.createFriendlyId(), "data/3.txt");

            Compose task = connections.spy(
                Compose.builder()
                    .id("compose-default-listingType")
                    .type(Compose.class.getName())
                    .list(
                        Compose.List.builder()
                            .from(Property.ofValue("gs://" + bucket + "/" + sourcePrefix))
                            .build()
                    )
                    .to(Property.ofValue("gs://" + bucket + "/" + outputObject))
                    .build()
            );

            RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());
            Compose.Output run = task.run(runContext);

            Blob composed = storage.get(bucket, outputObject);
            assertThat(composed, notNullValue());
            List<String> gcsLines = Arrays.asList(new String(composed.getContent(), StandardCharsets.UTF_8).split("\\R"));
            assertThat(gcsLines, containsInAnyOrder("1", "2", "3"));

            Download download = connections.spy(
                Download.builder()
                    .id(DownloadTest.class.getSimpleName())
                    .type(Download.class.getName())
                    .from(Property.ofValue(run.getUri().toString()))
                    .build()
            );

            downloaded = download.run(runContext).getUri();
            String result;
            try (InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, downloaded)) {
                result = CharStreams.toString(new InputStreamReader(get, StandardCharsets.UTF_8));
            }
            List<String> lines = Arrays.asList(result.split("\\R"));
            assertThat(lines, containsInAnyOrder("1", "2", "3"));
        } finally {
            try {
                if (downloaded != null) {
                    storageInterface.delete(TenantService.MAIN_TENANT, null, downloaded);
                }
            } finally {
                try {
                    deleteObjects(sourcePrefix);
                } finally {
                    storage.delete(bucket, outputObject);
                }
            }
        }
    }

    private void deleteObjects(String prefix) {
        storage.list(bucket, Storage.BlobListOption.prefix(prefix))
            .iterateAll()
            .forEach(blob -> storage.delete(blob.getBlobId()));
    }
}
