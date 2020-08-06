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
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatasetTest {
    private static String randomId = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.bigquery.project}")
    private String project;

    private RunContext runContext() {
        return runContext(randomId);
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

    @Test
    @Order(7)
    void acl() throws Exception {

        final String datasetId = "tu_createUpdateAcl_2HJEINNICW";

        CreateDataset task = createBuilder()
            .description(datasetId)
            .ifExists(CreateDataset.IfExists.UPDATE)
            .acl(Arrays.asList(
                AbstractDataset.AccessControl.builder()
                    .entity(AbstractDataset.AccessControl.Entity.builder()
                        .type(AbstractDataset.AccessControl.Entity.Type.GROUP)
                        .value("frlm-full-ddpddf@leroymerlin.fr").build())
                    .role(AbstractDataset.AccessControl.Role.READER)
                    .build(),
                AbstractDataset.AccessControl.builder()
                    .entity(AbstractDataset.AccessControl.Entity.builder()
                        .type(AbstractDataset.AccessControl.Entity.Type.GROUP)
                        .value("frlm-full-ddpdat@leroymerlin.fr").build())
                    .role(AbstractDataset.AccessControl.Role.READER)
                    .build(),
                AbstractDataset.AccessControl.builder()
                    .entity(AbstractDataset.AccessControl.Entity.builder()
                        .type(AbstractDataset.AccessControl.Entity.Type.USER)
                        .value("inhabitant-squad@lmfr-ddp-host-dev.iam.gserviceaccount.com").build())
                    .role(AbstractDataset.AccessControl.Role.OWNER)
                    .build()
            ))
            .build();

        RunContext rc = runContext(datasetId);
        AbstractDataset.Output run = task.run(rc);

        assertThat(run.getDataset(), is(rc.getVariables().get("dataset")));
        assertThat(run.getDescription(), is(datasetId));

        BigQuery connection = new BigQueryService().of(run.getProject(), "EU");

        // Test bigquery dataset acl ...
        Dataset dataset = connection.getDataset(run.getDataset());

        assertThat(null, not(dataset.getAcl()));
        assertThat(dataset.getAcl(), hasItems(
            Acl.of(new Acl.Group("frlm-full-ddpddf@leroymerlin.fr"), Acl.Role.READER),
            Acl.of(new Acl.Group("frlm-full-ddpdat@leroymerlin.fr"), Acl.Role.READER),
            Acl.of(new Acl.User("inhabitant-squad@lmfr-ddp-host-dev.iam.gserviceaccount.com"), Acl.Role.OWNER)
            )
        );
    }
}
