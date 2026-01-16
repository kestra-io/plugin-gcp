package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
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

    @Test
    void table() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
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
        assertThat(run.getDefinition().getSchema().getFields().size(), is(3));

        assertThat(run.getDefinition().getSchema().getFields().get(1).getType(), is(StandardSQLTypeName.INT64));
        assertThat(run.getDefinition().getStandardTableDefinition().getClustering().get(0), is("quantity"));
        assertThat(run.getDefinition().getStandardTableDefinition().getTimePartitioning().getField(), is("date"));
    }

    @Test
    void view() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
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
        assertThat(run.getDefinition().getSchema().getFields().size(), is(4));

        assertThat(run.getDefinition().getSchema().getFields().get(2).getType(), is(StandardSQLTypeName.FLOAT64));
    }


    @Test
    void dontExistsError() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        TableMetadata task = TableMetadata.builder()
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
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
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
            .ifNotExists(Property.ofValue(TableMetadata.IfNotExists.SKIP))
            .build();

        TableMetadata.Output run = task.run(runContextFactory.of(ImmutableMap.of()));

        assertThat(run.getTable(), is(nullValue()));
    }
}
