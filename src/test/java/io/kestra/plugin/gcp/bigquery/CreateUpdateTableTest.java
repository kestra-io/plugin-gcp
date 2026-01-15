package io.kestra.plugin.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.bigquery.models.Field;
import io.kestra.plugin.gcp.bigquery.models.Schema;
import io.kestra.plugin.gcp.bigquery.models.StandardTableDefinition;
import io.kestra.plugin.gcp.bigquery.models.TableDefinition;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@EnabledIfEnvironmentVariable(named = "GOOGLE_APPLICATION_CREDENTIALS", matches = ".+")
public class CreateUpdateTableTest {
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
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
            .friendlyName(Property.ofValue("new_table"))
            .tableDefinition(TableDefinition.builder()
                .type(Property.ofValue(TableDefinition.Type.TABLE))
                .schema(Schema.builder()
                    .fields(Arrays.asList(
                        Field.builder()
                            .name(Property.ofValue("id"))
                            .type(Property.ofValue(StandardSQLTypeName.INT64))
                            .build(),
                        Field.builder()
                            .name(Property.ofValue("name"))
                            .type(Property.ofValue(StandardSQLTypeName.STRING))
                            .build()
                    ))
                    .build()
                )
                .standardTableDefinition(StandardTableDefinition.builder()
                    .clustering(Property.ofValue(Arrays.asList("id", "name")))
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
            .projectId(Property.ofValue(this.project))
            .dataset(Property.ofValue(this.dataset))
            .table(Property.ofValue(friendlyId))
            .friendlyName(Property.ofValue("new_table_2"))
            .expirationDuration(Property.ofValue(Duration.ofHours(2)))
            .build();

        UpdateTable.Output updateRun = updateTask.run(runContext);

        assertThat(updateRun.getFriendlyName(), is("new_table_2"));
        assertThat(updateRun.getExpirationTime(), is(not(run.getExpirationTime())));
    }

    @Test
    void runWithoutStandardDefinition() throws Exception {
        String friendlyId = FriendlyId.createFriendlyId();

        CreateTable task = CreateTable.builder()
                .projectId(Property.ofValue(this.project))
                .dataset(Property.ofValue(this.dataset))
                .table(Property.ofValue(friendlyId))
                .friendlyName(Property.ofValue("new_table"))
                .tableDefinition(TableDefinition.builder()
                        .type(Property.ofValue(TableDefinition.Type.TABLE))
                        .schema(Schema.builder()
                                .fields(Arrays.asList(
                                        Field.builder()
                                                .name(Property.ofValue("id"))
                                                .type(Property.ofValue(StandardSQLTypeName.INT64))
                                                .build(),
                                        Field.builder()
                                                .name(Property.ofValue("name"))
                                                .type(Property.ofValue(StandardSQLTypeName.STRING))
                                                .build()
                                ))
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
    }
}
