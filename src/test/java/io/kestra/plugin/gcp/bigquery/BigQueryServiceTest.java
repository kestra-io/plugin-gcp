package io.kestra.plugin.gcp.bigquery;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class BigQueryServiceTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void labels() {
        var task = Query.builder()
            .id("query")
            .type(Query.class.getName())
            .sql("{{sql}}")
            .location("EU")
            .fetch(true)
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var labels = BigQueryService.labels(runContext);

        assertThat(labels.get("kestra_namespace"), is("io_kestra_plugin_gcp_bigquery_bigqueryservicetest"));
        assertThat(labels.get("kestra_flow_id"), is("labels"));
        assertThat(labels.get("kestra_execution_id"), notNullValue());
        assertThat(labels.get("kestra_task_id"), is("query"));
    }
}