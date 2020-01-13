package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.storages.StorageObject;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class CopyTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private ApplicationContext applicationContext;


    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void run() throws Exception {
        RunContext runContext = new RunContext(
            this.applicationContext,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );

        String in = FriendlyId.createFriendlyId();
        String out = FriendlyId.createFriendlyId();

        StorageObject source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        Upload upload = Upload.builder()
            .from(source.getUri().toString())
            .to("gs://{{bucket}}/tasks/gcp/copy/" + in + ".yml")
            .build();

        upload.run(runContext);

        Copy task = Copy.builder()
            .from("gs://{{bucket}}/tasks/gcp/copy/" + in + ".yml")
            .to("gs://{{bucket}}/tasks/gcp/copy/" + out + ".yml")
            .build();

        RunOutput run = task.run(runContext);

        assertThat(run.getOutputs().get("uri"), is(new URI("gs://" + bucket + "/tasks/gcp/copy/" + out + ".yml")));
    }
}