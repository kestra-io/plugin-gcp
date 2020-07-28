package org.kestra.task.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
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
class BucketTest {
    private static final String RANDOM_ID = "tu_" + FriendlyId.createFriendlyId().toLowerCase();

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

    @Test
    @Order(7)
    void acl() throws Exception {

        final String bucketId = "tu_bucket_test_acl_sqp0w6ojc";

        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .acl(Arrays.asList(
                AbstractBucket.AccessControl.builder()
                    .entity(AbstractBucket.AccessControl.Entity.builder()
                        .type(AbstractBucket.AccessControl.Entity.Type.GROUP)
                        .value("frlm-full-ddpddf@leroymerlin.fr").build())
                    .role(AbstractBucket.AccessControl.Role.READER)
                    .build(),
                AbstractBucket.AccessControl.builder()
                    .entity(AbstractBucket.AccessControl.Entity.builder()
                        .type(AbstractBucket.AccessControl.Entity.Type.GROUP)
                        .value("frlm-full-ddpdat@leroymerlin.fr").build())
                    .role(AbstractBucket.AccessControl.Role.READER)
                    .build(),
                AbstractBucket.AccessControl.builder()
                    .entity(AbstractBucket.AccessControl.Entity.builder()
                        .type(AbstractBucket.AccessControl.Entity.Type.USER)
                        .value("inhabitant-squad@lmfr-ddp-host-dev.iam.gserviceaccount.com").build())
                    .role(AbstractBucket.AccessControl.Role.OWNER)
                    .build()
            ))
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        RunContext rc = runContext(bucketId);

        AbstractBucket.Output run = task.run(rc);

        assertThat(run.getBucket(), is(rc.getVariables().get("bucket")));
        assertThat(run.getIndexPage(), is("createUpdate"));

        Storage connection = new Connection().of(rc.render(this.project));
        Bucket bucket = connection.get(run.getBucket());

        assertThat(null, not(bucket));

        assertThat(bucket.getAcl(), hasItems(
            matchingRecord(Acl.of(new Acl.Group("frlm-full-ddpddf@leroymerlin.fr"), Acl.Role.READER)),
            matchingRecord(Acl.of(new Acl.Group("frlm-full-ddpdat@leroymerlin.fr"), Acl.Role.READER)),
            matchingRecord(Acl.of(new Acl.User("inhabitant-squad@lmfr-ddp-host-dev.iam.gserviceaccount.com"), Acl.Role.OWNER))
        ));
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
