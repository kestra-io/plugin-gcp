package org.kestra.task.gcp.bigquery;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;

import javax.inject.Inject;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetTest {
    private static final String RANDOM_ID = "tu_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String RANDOM_ID_2 = "tu_" + FriendlyId.createFriendlyId();

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    private RunContext runContext() {
        return runContext(RANDOM_ID);
    }

    private RunContext runContext(String datasetId) {
        return runContextFactory.of(ImmutableMap.of(
            "project", this.project,
            "dataset", datasetId
        ));
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
    void acl() throws Exception {

        CreateDataset task = createBuilder()
            .description(RANDOM_ID_2)
            .ifExists(CreateDataset.IfExists.UPDATE)
            .acl(Collections.singletonList(
                AbstractDataset.AccessControl.builder()
                    .entity(AbstractDataset.AccessControl.Entity.builder()
                        .type(AbstractDataset.AccessControl.Entity.Type.USER)
                        .value("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com").build())
                    .role(AbstractDataset.AccessControl.Role.OWNER)
                    .build()
            ))
            .build();

        RunContext rc = runContext(RANDOM_ID_2);
        AbstractDataset.Output run = task.run(rc);

        assertThat(run.getDataset(), is(rc.getVariables().get("dataset")));
        assertThat(run.getDescription(), is(RANDOM_ID_2));

        BigQuery connection = task.connection(rc);

        // Test bigquery dataset acl ...
        Dataset dataset = connection.getDataset(run.getDataset());

        assertThat(null, not(dataset.getAcl()));
        assertThat(dataset.getAcl(), hasItems(
            Acl.of(new Acl.User("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com"), Acl.Role.OWNER)
        ));
    }

    @Test
    @Order(7)
    void delete() throws Exception {
        RunContext runContext = runContext();

        DeleteDataset task = DeleteDataset.builder()
            .id(DatasetTest.class.getSimpleName())
            .type(DeleteDataset.class.getName())
            .name("{{dataset}}")
            .projectId("{{project}}")
            .deleteContents(true)
            .build();

        DeleteDataset.Output run = task.run(runContext);
        assertThat(run.getDataset(), is(runContext.getVariables().get("dataset")));

        runContext = runContext(RANDOM_ID_2);

        task = DeleteDataset.builder()
            .id(DatasetTest.class.getSimpleName())
            .type(DeleteDataset.class.getName())
            .name("{{dataset}}")
            .projectId("{{project}}")
            .deleteContents(true)
            .build();

        run = task.run(runContext);
        assertThat(run.getDataset(), is(runContext.getVariables().get("dataset")));
    }
}
