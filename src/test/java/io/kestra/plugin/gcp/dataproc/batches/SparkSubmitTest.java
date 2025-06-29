package io.kestra.plugin.gcp.dataproc.batches;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Slf4j
@Disabled
class SparkSubmitTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.dataproc.project}")
    private String project;


    @Test
    void run() throws Exception {
        SparkSubmit submit = SparkSubmit.builder()
            // The example can be retrieved from the Spark site (version Scala 2.13) and must be uploaded to the GCS bucket before running the test
            .id(SparkSubmit.class.getSimpleName())
            .type(SparkSubmit.class.getName())
            .jarFileUris(Property.ofValue(List.of("gs://spark-jobs-kestra/spark-examples.jar")))
            .mainClass(Property.ofValue("org.apache.spark.examples.SparkPi"))
            .args(Property.ofValue(List.of("1000")))
            .projectId(Property.ofValue(project))
            .name(Property.ofValue("test-spark"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, submit, ImmutableMap.of());
        AbstractBatch.Output createOutput = submit.run(runContext);
        assertThat(createOutput.getState(), is(notNullValue()));
    }
}