package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        File file = new File(Objects.requireNonNull(DownloadTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI());

        URI source = storageInterface.put(
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(file)
        );

        String out = FriendlyId.createFriendlyId();

        Upload upload = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + ".yml")
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(uploadOutput.getUri().toString())
            .build();


        Download.Output run = task.run(runContext(task));
        assertThat(run.getUri().toString(), endsWith(".yml"));

        InputStream get = storageInterface.get(null, run.getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file))))
        );
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
