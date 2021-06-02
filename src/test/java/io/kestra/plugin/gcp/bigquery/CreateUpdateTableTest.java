package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.bigquery.models.Field;
import io.kestra.plugin.gcp.bigquery.models.Schema;
import io.kestra.plugin.gcp.bigquery.models.StandardTableDefinition;
import io.kestra.plugin.gcp.bigquery.models.TableDefinition;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@Slf4j
class CreateUpdateTableTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    @Value("${kestra.tasks.bigquery.dataset}")
    private String dataset;

    @Test
    void run() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        CreateTable task = CreateTable.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .friendlyName("new_table")
            .tableDefinition(TableDefinition.builder()
                .type(TableDefinition.Type.TABLE)
                .schema(Schema.builder()
                    .fields(Arrays.asList(
                        Field.builder()
                            .name("id")
                            .type(StandardSQLTypeName.INT64)
                            .build(),
                        Field.builder()
                            .name("name")
                            .type(StandardSQLTypeName.STRING)
                            .build()
                    ))
                    .build()
                )
                .standardTableDefinition(StandardTableDefinition.builder()
                    .clustering(Arrays.asList("id", "name"))
                    .build()
                )
                .build()
            )
            .build();
        RunContext runContext = runContextFactory.of(ImmutableMap.of());


        CreateTable.Output run = task.run(runContext);

        assertThat(run.getTable(), is(friendlyId));
        assertThat(run.getFriendlyName(), is("new_table"));
        assertThat(run.getDefinition().getSchema().getFields().size(), is(2));

        assertThat(run.getDefinition().getSchema().getFields().get(0).getType(), is(StandardSQLTypeName.INT64));
        assertThat(run.getDefinition().getStandardTableDefinition().getClustering().get(0), is("id"));

        assertThat(run.getExpirationTime(), notNullValue());

        UpdateTable updateTask = UpdateTable.builder()
            .projectId(this.project)
            .dataset(this.dataset)
            .table(friendlyId)
            .friendlyName("new_table_2")
            .expirationDuration(Duration.ofHours(2))
            .build();

        UpdateTable.Output updateRun = updateTask.run(runContext);

        assertThat(updateRun.getFriendlyName(), is("new_table_2"));
        assertThat(updateRun.getExpirationTime(), is(not(run.getExpirationTime())));
    }
}
