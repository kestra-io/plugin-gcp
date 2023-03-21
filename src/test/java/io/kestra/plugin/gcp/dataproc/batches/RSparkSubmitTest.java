package io.kestra.plugin.gcp.dataproc.batches;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@Slf4j
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
            .mainRFileUri("gs://spark-jobs-kestra/dataframe.r")
            .id(RSparkSubmit.class.getSimpleName())
            .type(RSparkSubmit.class.getName())
            .projectId(project)
            .name("test-rspark")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, ImmutableMap.of());
        AbstractBatch.Output createOutput = submit.run(runContext);
        assertThat(createOutput.getState(), is(notNullValue()));
    }
}