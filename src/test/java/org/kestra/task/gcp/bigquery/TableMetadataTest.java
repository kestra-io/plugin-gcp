package org.kestra.task.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@Slf4j
class TableMetadataTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    private Job query(BigQuery bigQuery, String query) throws InterruptedException {
        return bigQuery
            .create(JobInfo
                .newBuilder(QueryJobConfiguration.newBuilder(query).build())
                .setJobId(JobId.of(UUID.randomUUID().toString()))
                .build()
            )
            .waitFor();
    }

    @SuppressWarnings("unchecked")
    @Test
    void table() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .build();
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        query(
            task.connection(runContext),
            "CREATE TABLE `" + this.dataset + "." + friendlyId + "`" +
                "(product STRING, quantity INT64, date TIMESTAMP)" +
                " PARTITION BY DATE(date)" +
                " CLUSTER BY quantity" +
                " OPTIONS(" +
                "  expiration_timestamp=TIMESTAMP_ADD(" +
                "  CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)," +
                "  friendly_name=\"new_view\"," +
                "  description=\"a view that expires in 2 days\"," +
                "  labels=[(\"org_unit\", \"development\")]" +
                ");"
        );

        TableMetadata.Output run = task.run(runContext);

        assertThat(run.getTable(), is(friendlyId));
        assertThat(run.getFriendlyName(), is("new_view"));
        assertThat(((List<Map<String, Object>>) run.getDefinition().getSchema().get("fields")).size(), is(3));

        assertThat(((Map<String, String>) ((List<Map<String, Object>>) run.getDefinition().getSchema().get("fields")).get(1).get("type")).get("standardType"), is(StandardSQLTypeName.INT64.name()));
        assertThat(run.getDefinition().getStandardTableDefinition().getClustering().getFields().get(0), is("quantity"));
        assertThat(run.getDefinition().getStandardTableDefinition().getTimePartitioning().getField(), is("date"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void view() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .build();

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        query(
            task.connection(runContext),
            "CREATE VIEW `" + this.dataset + "." + friendlyId + "`\n" +
                "OPTIONS(" +
                "  expiration_timestamp=TIMESTAMP_ADD(" +
                "  CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)," +
                "  friendly_name=\"new_view\"," +
                "  description=\"a view that expires in 2 days\"," +
                "  labels=[(\"org_unit\", \"development\")]" +
                ")\n" +
                "AS SELECT 'name' as name, 'state' as state, 1.23 as float, 1 as int"
        );


        TableMetadata.Output run = task.run(runContext);

        assertThat(run.getTable(), is(friendlyId));
        assertThat(run.getFriendlyName(), is("new_view"));
        assertThat(((List<Map<String, Object>>) run.getDefinition().getSchema().get("fields")).size(), is(4));

        assertThat(((Map<String, String>) ((List<Map<String, Object>>) run.getDefinition().getSchema().get("fields")).get(2).get("type")).get("standardType"), is(StandardSQLTypeName.FLOAT64.name()));
    }


    @Test
    void dontExistsError() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .build();

        // flow is not created
        assertThrows(IllegalArgumentException.class, () -> {
            TableMetadata.Output run = task.run(runContextFactory.of(ImmutableMap.of()));
        });
    }

    @Test
    void dontExistsNoError() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .ifNotExists(TableMetadata.IfNotExists.SKIP)
            .build();

        TableMetadata.Output run = task.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(run.getTable(), is(nullValue()));
    }
}
