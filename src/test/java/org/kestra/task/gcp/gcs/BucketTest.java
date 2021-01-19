package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.*;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;

import javax.inject.Inject;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BucketTest {
    private static final String RANDOM_ID = "tu_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String RANDOM_ID_2 = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.project}")
    private String project;

    private RunContext runContext() {
        return runContext(RANDOM_ID);
    }

    private RunContext runContext(String bucketId) {
        return runContextFactory.of(ImmutableMap.of(
            "project", this.project,
            "bucket", bucketId
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
    void acl() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .acl(Collections.singletonList(
                AbstractBucket.AccessControl.builder()
                    .entity(AbstractBucket.AccessControl.Entity.builder()
                        .type(AbstractBucket.AccessControl.Entity.Type.USER)
                        .value("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com").build())
                    .role(AbstractBucket.AccessControl.Role.OWNER)
                    .build()
            ))
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        RunContext rc = runContext(RANDOM_ID_2);

        AbstractBucket.Output run = task.run(rc);

        assertThat(run.getBucket(), is(rc.getVariables().get("bucket")));
        assertThat(run.getIndexPage(), is("createUpdate"));

        Storage connection = task.connection(rc);
        Bucket bucket = connection.get(run.getBucket());

        assertThat(null, not(bucket));

        assertThat(bucket.getAcl(), hasItems(
            matchingRecord(Acl.of(new Acl.User("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com"), Acl.Role.OWNER))
        ));
    }

    @Test
    @Order(7)
    void delete() throws Exception {
        RunContext runContext = runContext();

        DeleteBucket task = DeleteBucket.builder()
            .id(BucketTest.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}")
            .build();

        DeleteBucket.Output run = task.run(runContext);
        assertThat(run.getBucket(), is(runContext.getVariables().get("bucket")));

        runContext = runContext(RANDOM_ID_2);

        task = DeleteBucket.builder()
            .id(BucketTest.class.getSimpleName())
            .type(DeleteBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}")
            .build();

        run = task.run(runContext);
        assertThat(run.getBucket(), is(runContext.getVariables().get("bucket")));
    }

    // Custom match for Acl
    private Matcher<Acl> matchingRecord(Acl expected) {
        return new TypeSafeMatcher<Acl>() {
            @Override
            public void describeTo(Description description) {
                description.appendText(expected.toString());
            }

            @Override
            protected boolean matchesSafely(Acl actual) {
                return actual.getEntity().equals(expected.getEntity())
                    && actual.getRole().equals(expected.getRole());
            }
        };

    }
}
