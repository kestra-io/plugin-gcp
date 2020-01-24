package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.storages.StorageObject;
import org.kestra.core.utils.TestsUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class UploadTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private ApplicationContext applicationContext;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        StorageObject source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        String out = FriendlyId.createFriendlyId();

        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.getUri().toString())
            .to("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + ".yml")
            .build();

        Upload.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(new URI("gs://" +  bucket + "/tasks/gcp/upload/"+ out + ".yml")));
    }

    @Test
    void fromRemoteUrl() throws Exception {
        String out = FriendlyId.createFriendlyId();

        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from("http://www.google.com")
            .to("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + ".html")
            .build();

        Upload.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(new URI("gs://" +  bucket + "/tasks/gcp/upload/" + out + ".html")));
    }

    private RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.applicationContext,
            task,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );
    }
}