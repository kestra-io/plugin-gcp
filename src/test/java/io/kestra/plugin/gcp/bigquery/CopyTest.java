package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
class CopyTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        var tableValue = project + "." + dataset + "." + FriendlyId.createFriendlyId();

        Query create = Query.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue(QueryTest.sql()))
            .destinationTable(Property.ofValue(tableValue))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, create, ImmutableMap.of());
        Query.Output createOutput = create.run(runContext);
        assertThat(createOutput.getJobId(), is(notNullValue()));

        Copy copy = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .projectId(Property.ofValue(project))
            .sourceTables(Property.ofValue(List.of(tableValue)))
            .destinationTable(Property.ofValue(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, copy, ImmutableMap.of());
        Copy.Output copyOutput = copy.run(runContext);
        assertThat(copyOutput.getJobId(), is(notNullValue()));


        Query fetch = Query.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Query.class.getName())
            .projectId(Property.ofValue(project))
            .sql(Property.ofValue("SELECT * FROM " + create.getDestinationTable()))
            .fetchOne(true)
            .build();

        runContext = TestsUtils.mockRunContext(runContextFactory, fetch, ImmutableMap.of());
        Query.Output fetchOutput = fetch.run(runContext);
        assertThat(fetchOutput.getRow().get("int"), is(1L));
    }
}