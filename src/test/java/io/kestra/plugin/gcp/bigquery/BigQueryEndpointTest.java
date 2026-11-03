package io.kestra.plugin.gcp.bigquery;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class BigQueryEndpointTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void endpointOverridesHostAndSkipsCredentials() throws Exception {
        Query task = Query.builder()
            .id("query")
            .type(Query.class.getName())
            .projectId(Property.ofValue("qa-proj"))
            .endpoint(Property.ofValue("http://localhost:4588"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        BigQuery bigQuery = task.connection(runContext);

        assertThat(bigQuery.getOptions().getHost(), is("http://localhost:4588"));
        assertThat(bigQuery.getOptions().getCredentials(), is(NoCredentials.getInstance()));
        assertThat(bigQuery.getOptions().getProjectId(), is("qa-proj"));
    }
}
