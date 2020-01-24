package org.kestra.task.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.kestra.core.runners.RunContext;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetTest {
    private static String randomId = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private ApplicationContext applicationContext;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    private RunContext runContext() {
        return new RunContext(
            this.applicationContext,
            ImmutableMap.of(
                "project", this.project,
                "dataset", randomId
            )
        );
    }

    private CreateDataset.CreateDatasetBuilder<?, ?> createBuilder() {
        return CreateDataset.builder()
            .id(DatasetTest.class.getSimpleName())
            .type(CreateDataset.class.getName())
            .name("{{dataset}}")
            .projectId("{{project}}");
    }

    @Test
    @Order(1)
    void create() throws Exception {
        CreateDataset task = createBuilder().build();
        AbstractDataset.Output run = task.run(runContext());

        assertThat(run.getDataset(), is(runContext().getVariables().get("dataset")));
    }

    @Test
    @Order(2)
    void createException() {
        CreateDataset task = createBuilder().build();

        assertThrows(BigQueryException.class, () -> {
            task.run(runContext());
        });
    }

    @Test
    @Order(3)
    void createNoException() throws Exception {
        CreateDataset task = createBuilder()
            .description("createUpdate")
            .ifExists(CreateDataset.IfExists.SKIP)
            .build();

        AbstractDataset.Output run = task.run(runContext());

        assertThat(run.getDataset(), is(runContext().getVariables().get("dataset")));
    }

    @Test
    @Order(4)
    void createUpdate() throws Exception {
        CreateDataset task = createBuilder()
            .description("createUpdate")
            .ifExists(CreateDataset.IfExists.UPDATE)
            .build();

        AbstractDataset.Output run = task.run(runContext());

        assertThat(run.getDataset(), is(runContext().getVariables().get("dataset")));
        assertThat(run.getDescription(), is("createUpdate"));
    }

    @Test
    @Order(5)
    void update() throws Exception {
        UpdateDataset task = UpdateDataset.builder()
            .id(UpdateDataset.class.getSimpleName())
            .type(CreateDataset.class.getName())
            .name("{{dataset}}")
            .projectId("{{project}}")
            .description("update")
            .build();

        AbstractDataset.Output run = task.run(runContext());

        assertThat(run.getDataset(), is(runContext().getVariables().get("dataset")));
        assertThat(run.getDescription(), is("update"));
    }

    @Test
    @Order(6)
    void delete() throws Exception {
        DeleteDataset task = DeleteDataset.builder()
            .id(DatasetTest.class.getSimpleName())
            .type(DeleteDataset.class.getName())
            .name("{{dataset}}")
            .projectId("{{project}}")
            .deleteContents(true)
            .build();

        DeleteDataset.Output run = task.run(runContext());
        assertThat(run.getDataset(), is(runContext().getVariables().get("dataset")));
    }
}