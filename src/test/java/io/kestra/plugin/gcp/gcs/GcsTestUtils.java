package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class GcsTestUtils {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    Upload.Output upload(String out) throws Exception {
        return this.upload(out, "application.yml");
    }

    Upload.Output upload(String out, String resource) throws Exception {
        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                .getResource(resource))
                .toURI()))
        );

        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + "." + FilenameUtils.getExtension(resource))
            .build();

        return task.run(runContext(task));
    }

    RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of(
                "bucket", this.bucket
            )
        );
    }
}
