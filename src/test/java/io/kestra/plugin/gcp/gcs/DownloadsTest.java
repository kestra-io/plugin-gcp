package io.kestra.plugin.gcp.gcs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.gcs.Upload.Output;
import io.kestra.plugin.gcp.gcs.models.Blob;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Inject
    private GcsTestUtils testUtils;

    private final String random = IdUtils.create();

    @Test
    void run() throws Exception {
        String out1 = FriendlyId.createFriendlyId();
        Output output = testUtils.upload(random + "/" + out1 + " (1).yml");
        String out2 = FriendlyId.createFriendlyId();
        testUtils.upload(random + "/" + out2);

        Downloads task = Downloads.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + random + "/"))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(output.getUri().toString(), run.getBlobs().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));
    }

    @Test
    void runMoveRecursive() throws Exception {
        var dir = random + "/move_recursive";
        testUtils.upload(dir + "/sub_a/file.yml");
        testUtils.upload(dir + "/sub_b/file.yml");

        var archiveDir = "gs://" + bucket + "/tasks/gcp/upload/" + random + "/archive";

        var task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + dir + "/"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveDirectory(Property.ofValue(archiveDir))
            .listingType(Property.ofValue(ListInterface.ListingType.RECURSIVE))
            .build();

        var run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        var listSource = List.builder()
            .id(DownloadsTest.class.getSimpleName() + "_source")
            .type(List.class.getName())
            .from(Property.ofValue("gs://" + bucket + "/tasks/gcp/upload/" + dir + "/"))
            .listingType(Property.ofValue(ListInterface.ListingType.RECURSIVE))
            .build();
        var sourceRun = listSource.run(runContext(listSource));
        assertThat(sourceRun.getBlobs().size(), is(0));

        var listArchive = List.builder()
            .id(DownloadsTest.class.getSimpleName() + "_archive")
            .type(List.class.getName())
            .from(Property.ofValue(archiveDir + "/"))
            .listingType(Property.ofValue(ListInterface.ListingType.RECURSIVE))
            .build();
        var archiveRun = listArchive.run(runContext(listArchive));
        assertThat(archiveRun.getBlobs().size(), is(2));
        var archiveNames = archiveRun.getBlobs().stream().map(Blob::getName).toList();
        assertThat(archiveNames.stream().anyMatch(n -> n.endsWith("/sub_a/file.yml")), is(true));
        assertThat(archiveNames.stream().anyMatch(n -> n.endsWith("/sub_b/file.yml")), is(true));
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
