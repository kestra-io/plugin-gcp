package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BucketTest {
    private static final String RANDOM_ID = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.project}")
    private String project;

    private RunContext runContext() {
        return runContextFactory.of(ImmutableMap.of(
            "project", this.project,
            "bucket", RANDOM_ID
        ));
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
        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(2)
    void createException() {
        CreateBucket task = createBuilder().build();

        assertThrows(RuntimeException.class, () -> {
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

        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(4)
    void createUpdate() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
        assertThat(run.getIndexPage(), is("createUpdate"));
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

        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
        assertThat(run.getIndexPage(), is("update"));
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

        DeleteBucket.Output run = task.run(runContext());
        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
    }
}
