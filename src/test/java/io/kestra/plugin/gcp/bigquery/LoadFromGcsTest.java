package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.gcp.gcs.Upload;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class LoadFromGcsTest {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Value("${kestra.tasks.gcs.bucket}")
    private String bucket;

    @Test
    void fromJson() throws Exception {
        File applicationFile = new File(Objects.requireNonNull(LoadFromGcsTest.class.getClassLoader()
            .getResource("data/us-states.json"))
            .toURI()
        );

        URI put = storageInterface.put(
            null,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );

        Upload upload = Upload.builder()
            .id(LoadFromGcsTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.of(put.toString()))
            .to(Property.of("gs://" + this.bucket + "/" + FriendlyId.createFriendlyId() + ".json"))
            .build();

        upload.run(TestsUtils.mockRunContext(this.runContextFactory, upload, ImmutableMap.of()));

        LoadFromGcs task = LoadFromGcs.builder()
            .id(LoadFromGcsTest.class.getSimpleName())
            .type(LoadFromGcs.class.getName())
            .from(Property.of(Collections.singletonList(
                upload.getTo().toString()
            )))
            .destinationTable(Property.of(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .format(AbstractLoad.Format.JSON)
            .schema(Property.of(ImmutableMap.of(
                "fields", Arrays.asList(
                    ImmutableMap.of("name", "name", "type", "STRING"),
                    ImmutableMap.of("name", "post_abbr", "type", "STRING")
                )
            )))
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        AbstractLoad.Output run = task.run(runContext);
        assertThat(run.getRows(), is(50L));
    }
}
