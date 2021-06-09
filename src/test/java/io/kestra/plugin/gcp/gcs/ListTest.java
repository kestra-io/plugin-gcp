package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.models.Blob;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
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
            lastFileName = upload("/tasks/gcp/" + dir);
        }
        upload("/tasks/gcp/" + dir + "/sub");

        // directory listing
        List task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .build();
        List.Output run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(11));
        assertThat(run.getBlobs().stream().filter(Blob::isDirectory).count(), is(1l));

        // only dir
        task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .filter(ListInterface.Filter.DIRECTORY)
            .listingType(ListInterface.ListingType.DIRECTORY)
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(1));
        assertThat(run.getBlobs().get(0).isDirectory(), is(true));

        // files only
        task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .filter(ListInterface.Filter.FILES)
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(10));

        // recursive
        task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .listingType(ListInterface.ListingType.RECURSIVE)
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(11));
        run.getBlobs().forEach(blob -> assertThat(blob.isDirectory(), is(false)));

        // regexp
        task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .filter(ListInterface.Filter.FILES)
            .listingType(ListInterface.ListingType.DIRECTORY)
            .regExp(".*\\/" + dir + "\\/.*")
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(10));

        // regexp on file
        task = task()
            .from("gs://" + this.bucket + "/tasks/gcp/" + dir + "/")
            .filter(ListInterface.Filter.FILES)
            .listingType(ListInterface.ListingType.DIRECTORY)
            .regExp(".*\\/" + dir + "\\/" + lastFileName + "\\+\\(1\\).(yaml|yml)")
            .build();
        run = task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getBlobs().size(), is(1));
    }

    private static List.ListBuilder<?, ?> task() {
        return List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName());
    }

    private String upload(String dir) throws Exception {
        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(ListTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        String out = FriendlyId.createFriendlyId();

        Upload task = Upload.builder()
            .id(ListTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("gs://" + this.bucket +  dir + "/" + out + " (1).yml")
            .build();

        task.run(TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of()));

        return out;
    }
}
