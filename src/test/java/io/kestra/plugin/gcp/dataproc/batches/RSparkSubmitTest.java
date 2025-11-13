package io.kestra.plugin.gcp.dataproc.batches;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled
class RSparkSubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;


    @Test
    void run() throws Exception {
        RSparkSubmit submit = RSparkSubmit.builder()
            // The file can be found in src/main/resources/dataproc and must be uploaded to the GCS bucket before running the test
            .id(RSparkSubmit.class.getSimpleName())
            .type(RSparkSubmit.class.getName())
            .mainRFileUri(Property.ofValue("gs://spark-jobs-kestra/dataframe.r"))
            .projectId(Property.ofValue(project))
            .name(Property.ofValue("test-rspark"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, ImmutableMap.of());
        AbstractBatch.Output createOutput = submit.run(runContext);
        assertThat(createOutput.getState(), is(notNullValue()));
    }
}