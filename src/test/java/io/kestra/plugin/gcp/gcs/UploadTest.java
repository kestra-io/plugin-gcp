package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
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
}
