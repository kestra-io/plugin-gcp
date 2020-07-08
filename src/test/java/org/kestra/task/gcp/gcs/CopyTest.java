package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class CopyTest {
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

        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        Upload upload = Upload.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml")
            .build();

        upload.run(runContext(upload));

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml")
            .to("gs://{{inputs.bucket}}/tasks/gcp/copy/" + out + ".yml")
            .build();

        Copy.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(new URI("gs://" + bucket + "/tasks/gcp/copy/" + out + ".yml")));
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
