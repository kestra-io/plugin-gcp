package io.kestra.plugin.gcp.gcs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URI;
import org.junit.jupiter.api.Test;

@MicronautTest
class UploadTest {
    @Inject private GcsTestUtils testUtils;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        String out = FriendlyId.createFriendlyId();
        Upload.Output run = testUtils.upload(out);

        assertThat(
                run.getUri(), is(new URI("gs://" + bucket + "/tasks/gcp/upload/" + out + ".yml")));
    }
}
