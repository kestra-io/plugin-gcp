package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ListTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void run() throws Exception {
        String dir = FriendlyId.createFriendlyId();
        String lastFileName = null;

        for (int i = 0; i < 10; i++) {
            lastFileName = upload(storageInterface, bucket, runContextFactory, "/tasks/gcp/" + dir);
        }
        upload(storageInterface, bucket, runContextFactory, "/tasks/gcp/" + dir + "/sub");

        // directory listing
        List task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .build();
        List.Output run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(11));
        assertThat(run.getBlobs().stream().filter(Blob::isDirectory).count(), is(1l));

        // only dir
        task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .filter(Property.ofValue(ListInterface.Filter.DIRECTORY))
            .listingType(Property.ofValue(ListInterface.ListingType.DIRECTORY))
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(1));
        assertThat(run.getBlobs().get(0).isDirectory(), is(true));

        // files only
        task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(10));

        // recursive
        task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .listingType(Property.ofValue(ListInterface.ListingType.RECURSIVE))
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(11));
        run.getBlobs().forEach(blob -> assertThat(blob.isDirectory(), is(false)));

        // regexp
        task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .listingType(Property.ofValue(ListInterface.ListingType.DIRECTORY))
            .regExp(Property.ofValue(".*\\/" + dir + "\\/.*"))
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(10));

        // regexp on file
        task = task()
            .from(Property.ofValue("gs://" + this.bucket + "/tasks/gcp/" + dir + "/"))
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .listingType(Property.ofValue(ListInterface.ListingType.DIRECTORY))
            .regExp(Property.ofValue(".*\\/" + dir + "\\/" + lastFileName + "\\+\\(1\\).(yaml|yml)"))
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(1));
    }

    private static List.ListBuilder<?, ?> task() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName());
    }

    static String upload(StorageInterface storageInterface, String bucket, RunContextFactory runContextFactory, String dir) throws Exception {
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(ListTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        String out = FriendlyId.createFriendlyId();

        Upload task = Upload.builder()
            .id(ListTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue("gs://" + bucket +  dir + "/" + out + " (1).yml"))
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        return out;
    }
}
