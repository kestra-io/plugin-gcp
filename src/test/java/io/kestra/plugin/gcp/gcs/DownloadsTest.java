package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
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
        testUtils.upload(random + "/" + out1 + " (1).yml");
        String out2 = FriendlyId.createFriendlyId();
        testUtils.upload(random + "/" + out2);

        Downloads task = Downloads.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .from("gs://" + bucket + "/tasks/gcp/upload/" + random + "/")
            .action(ActionInterface.Action.DELETE)
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));
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
