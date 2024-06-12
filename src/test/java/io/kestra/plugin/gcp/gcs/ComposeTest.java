package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ComposeTest {
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

        testUtils.upload("compose-" + dir + "/compose1/" + FriendlyId.createFriendlyId(), "data/1.txt");
        testUtils.upload("compose-" + dir + "/compose2/" + FriendlyId.createFriendlyId(), "data/2.txt");
        testUtils.upload("compose-" + dir + "/compose3/" + FriendlyId.createFriendlyId(), "data/3.txt");

        Compose task = Compose.builder()
            .id(ComposeTest.class.getSimpleName())
            .type(Compose.class.getName())
            .list(Compose.List.builder()
                .from("gs://" +  bucket + "/tasks/gcp/upload/compose-" + dir + "/")
                .listingType(ListInterface.ListingType.RECURSIVE)
                .build()
            )
            .to("gs://" +  bucket + "/tasks/gcp/compose-result/compose.txt")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());
        Compose.Output run = task.run(runContext);

        Download download = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(run.getUri().toString())
            .build();

        InputStream get = storageInterface.get(null, download.run(runContext).getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is("1\n2\n3\n")
        );

    }
}
