package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
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
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                .getResource("application.yml"))
                .toURI()))
        );

        Upload upload = Upload.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .build();

        upload.run(runContext(upload));

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + out + ".yml"))
            .build();

        Copy.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(new URI("gs://" + bucket + "/tasks/gcp/copy/" + out + ".yml")));
    }

    @Test
    void sameException() {
        String in = FriendlyId.createFriendlyId();

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/copy/" + in + ".yml"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            task.run(runContext(task));
        });
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
