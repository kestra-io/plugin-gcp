package io.kestra.plugin.gcp.dataproc.clusters;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Slf4j
@Disabled
public class DeleteTest {

	@Inject
	private RunContextFactory runContextFactory;

	@Value("${kestra.tasks.dataproc.project}")
	private String project;

	@Value("${kestra.tasks.dataproc.region}")
	private String region;

    @Test
    void run() throws Exception {
        Delete create = Delete.builder()
            .id(Delete.class.getSimpleName())
            .type(Delete.class.getName())
            .projectId(Property.ofValue(project))
            .clusterName("test-cluster")
	        .region(region)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());

        Delete.Output createOutput = create.run(runContext);
        assertThat(createOutput.isDeleted(), is(true));
    }

}
