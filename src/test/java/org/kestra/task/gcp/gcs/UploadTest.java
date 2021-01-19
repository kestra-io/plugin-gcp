package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class UploadTest {
    @Inject
    private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        String out = FriendlyId.createFriendlyId();
        Upload.Output run = testUtils.upload(out);

        assertThat(run.getUri(), is(new URI("gs://" +  bucket + "/tasks/gcp/upload/" + out + ".yml")));
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

        Upload.Output run = task.run(testUtils.runContext(task));

        assertThat(run.getUri(), is(new URI("gs://" +  bucket + "/tasks/gcp/upload/" + out + ".html")));
    }
}
