package io.kestra.plugin.gcp.gcs;

import java.util.Collections;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.gcs.models.AccessControl;
import io.kestra.plugin.gcp.gcs.models.Entity;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Floci 0.11.0 does not faithfully emulate bucket website index-page metadata or inline bucket ACL behavior.
 * {@code createUpdate()} and {@code update()} stay credential-gated for website metadata, and {@code acl()} stays
 * credential-gated for inline bucket ACL behavior. Emulator-compatible cases live in {@link FlociBucketTest}.
 */
@KestraTest
@EnabledIfEnvironmentVariable(
    named = "GOOGLE_APPLICATION_CREDENTIALS",
    matches = ".+",
    disabledReason = "Floci 0.11.0 does not faithfully emulate bucket website index-page metadata or inline bucket ACL behavior."
)
class BucketTest {
    private static final String CREATE_UPDATE_BUCKET = "tu_create_update_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String UPDATE_BUCKET = "tu_update_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String ACL_BUCKET = "tu_acl_" + FriendlyId.createFriendlyId().toLowerCase();

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.gcs.project}")
    private String project;

    private RunContext runContext(String bucketId) {
        return runContextFactory.of(
            ImmutableMap.of(
                "project", this.project,
                "bucket", bucketId
            )
        );
    }

    private CreateBucket.CreateBucketBuilder<?, ?> createBuilder() {
        return CreateBucket.builder()
            .id(BucketTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .name(Property.ofExpression("{{bucket}}"))
            .projectId(Property.ofValue(project));
    }

    private void createExistingBucket(String bucketId) throws Exception {
        CreateBucket task = createBuilder().build();
        try (Storage connection = task.connection(runContext(bucketId))) {
            connection.create(BucketInfo.of(bucketId));
        }
    }

    private void cleanUpBucket(String bucketId) throws Exception {
        CreateBucket task = createBuilder().build();
        try (Storage connection = task.connection(runContext(bucketId))) {
            if (connection.get(bucketId) != null) {
                connection.delete(bucketId);
            }
        }
    }

    @Test
    void createUpdate() throws Exception {
        try {
            createExistingBucket(CREATE_UPDATE_BUCKET);

            CreateBucket task = createBuilder()
                .indexPage(Property.ofValue("createUpdate"))
                .ifExists(Property.ofValue(CreateBucket.IfExists.UPDATE))
                .build();

            RunContext runContext = runContext(CREATE_UPDATE_BUCKET);
            AbstractBucket.Output run = task.run(runContext);

            assertThat(run.getBucket().getName(), is(runContext.getVariables().get("bucket")));
            assertThat(run.getBucket().getIndexPage(), is("createUpdate"));
        } finally {
            cleanUpBucket(CREATE_UPDATE_BUCKET);
        }
    }

    @Test
    void update() throws Exception {
        try {
            createExistingBucket(UPDATE_BUCKET);

            UpdateBucket task = UpdateBucket.builder()
                .id(UpdateBucket.class.getSimpleName())
                .type(CreateBucket.class.getName())
                .name(Property.ofExpression("{{bucket}}"))
                .projectId(Property.ofExpression("{{project}}"))
                .indexPage(Property.ofValue("update"))
                .build();

            RunContext runContext = runContext(UPDATE_BUCKET);
            AbstractBucket.Output run = task.run(runContext);

            assertThat(run.getBucket().getName(), is(runContext.getVariables().get("bucket")));
            assertThat(run.getBucket().getIndexPage(), is("update"));
        } finally {
            cleanUpBucket(UPDATE_BUCKET);
        }
    }

    @Test
    void acl() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage(Property.ofValue("createUpdate"))
            .acl(
                Collections.singletonList(
                    AccessControl.builder()
                        .entity(
                            Entity.builder()
                                .type(Property.ofValue(Entity.Type.USER))
                                .value(Property.ofValue("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com")).build()
                        )
                        .role(Property.ofValue(AccessControl.Role.OWNER))
                        .build()
                )
            )
            .ifExists(Property.ofValue(CreateBucket.IfExists.UPDATE))
            .build();

        RunContext rc = runContext(ACL_BUCKET);

        try {
            AbstractBucket.Output run = task.run(rc);

            assertThat(run.getBucket().getName(), is(rc.getVariables().get("bucket")));
            assertThat(run.getBucket().getIndexPage(), is("createUpdate"));

            try (Storage connection = task.connection(rc)) {
                Bucket bucket = connection.get(run.getBucket().getName());

                assertThat(null, not(bucket));

                assertThat(
                    bucket.getAcl(), hasItems(
                        matchingAcl(Acl.of(new Acl.User("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com"), Acl.Role.OWNER))
                    )
                );
            }
        } finally {
            cleanUpBucket(ACL_BUCKET);
        }
    }

    // Custom match for Acl
    private Matcher<Acl> matchingAcl(Acl expected) {
        return new TypeSafeMatcher<>() {
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
