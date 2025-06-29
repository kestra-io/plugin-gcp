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

@KestraTest
class DeleteTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromStorage() throws Exception {
        File file = new File(Objects.requireNonNull(DeleteTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI());

        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(file)
        );

        String out = FriendlyId.createFriendlyId();

        Upload upload = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue("gs://{{inputs.bucket}}/tasks/gcp/upload/" + out + ".yml"))
            .build();

        Upload.Output uploadOutput = upload.run(runContext(upload));

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Download.class.getName())
            .uri(Property.ofValue(uploadOutput.getUri().toString()))
            .build();


        Delete.Output run = task.run(runContext(task));

        assertThat(run.getDeleted(), is(true));

        run = task.run(runContext(task));

        assertThat(run.getDeleted(), is(false));
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
