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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
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

    @Test
    void labelsAreNotOverwritten() throws Exception {
        var tableValue = project + "." + dataset + "." + FriendlyId.createFriendlyId();

        Map<String, String> initialLabels = new HashMap<>();
        initialLabels.put("env", "test");
        initialLabels.put("engine", "bigquery");

        Copy task = Copy.builder()
            .id(Copy.class.getSimpleName())
            .type(Copy.class.getName())
            .projectId(Property.ofValue(project))
            .sourceTables(Property.ofValue(List.of(tableValue)))
            .destinationTable(Property.ofValue(project + "." + dataset + "." + FriendlyId.createFriendlyId()))
            .labels(Property.ofValue(initialLabels))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());

        var labels = task.jobConfiguration(runContext).getLabels();

        assertThat(labels.size(), is(6));
        assertThat(labels.get("env"), is("test"));
        assertThat(labels.get("engine"), is("bigquery"));
        assertThat(labels.get("kestra_namespace"), is("io_kestra_plugin_gcp_bigquery_copytest"));
        assertThat(labels.get("kestra_flow_id"), is("labelsarenotoverwritten"));
        assertThat(labels.get("kestra_execution_id"), notNullValue());
        assertThat(labels.get("kestra_task_id"), is("copy"));
    }
}