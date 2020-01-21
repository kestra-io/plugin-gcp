package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BucketTest {
    private static String randomId = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private ApplicationContext applicationContext;

    @Value("${kestra.tasks.gcs.project}")
    private String project;

    private RunContext runContext() {
        return new RunContext(
            this.applicationContext,
            ImmutableMap.of(
                "project", this.project,
                "bucket", randomId
            )
        );
    }

    private CreateBucket.CreateBucketBuilder<?, ?> createBuilder() {
        return CreateBucket.builder()
            .id(BucketTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}");
    }

    @Test
    @Order(1)
    void create() throws Exception {
        CreateBucket task = createBuilder().build();
        RunOutput run = task.run(runContext());

        assertThat(run.getOutputs().get("bucket"), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(2)
    void createException() {
        CreateBucket task = createBuilder().build();

        assertThrows(StorageException.class, () -> {
            task.run(runContext());
        });
    }

    @Test
    @Order(3)
    void createNoException() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .ifExists(CreateBucket.IfExists.SKIP)
            .build();

        RunOutput run = task.run(runContext());

        assertThat(run.getOutputs().get("bucket"), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(4)
    void createUpdate() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        RunOutput run = task.run(runContext());

        assertThat(run.getOutputs().get("bucket"), is(runContext().getVariables().get("bucket")));
        assertThat(run.getOutputs().get("indexPage"), is("createUpdate"));
    }

    @Test
    @Order(5)
    void update() throws Exception {
        UpdateBucket task = UpdateBucket.builder()
            .id(UpdateBucket.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}")
            .indexPage("update")
            .build();

        RunOutput run = task.run(runContext());

        assertThat(run.getOutputs().get("bucket"), is(runContext().getVariables().get("bucket")));
        assertThat(run.getOutputs().get("indexPage"), is("update"));
    }

    @Test
    @Order(6)
    void delete() throws Exception {
        DeleteBucket task = DeleteBucket.builder()
            .id(BucketTest.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}")
            .build();

        RunOutput run = task.run(runContext());
        assertThat(run.getOutputs().get("bucket"), is(runContext().getVariables().get("bucket")));
    }
}