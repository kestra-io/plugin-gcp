package io.kestra.plugin.gcp.gcs;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo.LifecycleRule;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kestra.plugin.gcp.gcs.models.AccessControl;
import io.kestra.plugin.gcp.gcs.models.BucketLifecycleRule;
import io.kestra.plugin.gcp.gcs.models.Entity;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import java.util.Collections;
import java.util.stream.Collectors;

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
            .name("{{bucket}}")
            .projectId("{{project}}");
    }

    @Test
    @Order(1)
    void create() throws Exception {
        CreateBucket task = createBuilder().build();
        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
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

        assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(4)
    void createUpdate() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        AbstractBucket.Output run = task.run(runContext());

        assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
        assertThat(run.getBucket().getIndexPage(), is("createUpdate"));
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

        assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
        assertThat(run.getBucket().getIndexPage(), is("update"));
    }

    @Test
    @Order(6)
    void acl() throws Exception {
        CreateBucket task = createBuilder()
            .indexPage("createUpdate")
            .acl(
                Collections.singletonList(
                    AccessControl.builder()
                        .entity(
                            Entity.builder()
                                .type(Entity.Type.USER)
                                .value("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com").build()
                        )
                        .role(AccessControl.Role.OWNER)
                        .build()
                )
            )
            .ifExists(CreateBucket.IfExists.UPDATE)
            .build();

        RunContext rc = runContext(RANDOM_ID_2);

        AbstractBucket.Output run = task.run(rc);

        assertThat(run.getBucket().getName(), is(rc.getVariables().get("bucket")));
        assertThat(run.getBucket().getIndexPage(), is("createUpdate"));

        Storage connection = task.connection(rc);
        Bucket bucket = connection.get(run.getBucket().getName());

        assertThat(null, not(bucket));

        assertThat(
            bucket.getAcl(), hasItems(
                matchingAcl(Acl.of(new Acl.User("kestra-unit-test@kestra-unit-test.iam.gserviceaccount.com"), Acl.Role.OWNER))
            )
        );
    }

    @Test
    @Order(7)
    void iamPolicy() throws Exception {
        CreateBucketIamPolicy.CreateBucketIamPolicyBuilder<?, ?> builder = CreateBucketIamPolicy.builder()
            .id(UpdateBucket.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .name("{{bucket}}")
            .projectId("{{project}}")
            .member("domain:kestra.io")
            .role("roles/storage.objectViewer");

        CreateBucketIamPolicy.Output run = builder.build().run(runContext());
        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));

        assertThrows(Exception.class, () -> {
            builder.ifExists(CreateBucketIamPolicy.IfExists.ERROR).build().run(runContext());
        });

        run = builder.ifExists(CreateBucketIamPolicy.IfExists.SKIP).build().run(runContext());
        assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
    }

    @Test
    @Order(8)
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

    @SuppressWarnings("unchecked")
    private void createBucketWithLifecycleRule(RunContext rc, java.util.List<BucketLifecycleRule> rules) throws Exception {
        CreateBucket task = createBuilder()
            .lifecycleRules(rules)
            .ifExists(CreateBucket.IfExists.ERROR)
            .build();

        AbstractBucket.Output run = task.run(rc);

        assertThat(run.getBucket().getName(), is(rc.getVariables().get("bucket")));

        Storage connection = task.connection(rc);
        Bucket bucket = connection.get(run.getBucket().getName());

        assertThat(null, not(bucket));

        assertThat(bucket.getLifecycleRules(), notNullValue());
        assertThat(bucket.getLifecycleRules().size(), is(rules.size()));

        java.util.List<Matcher<LifecycleRule>> matchers = task
            .getLifecycleRules()
            .stream()
            .map(rule -> matchingLifecycleRuleOnTypeAndAge(rule.convert()))
            .collect(Collectors.toList());

        assertThat(matchers.size(), is(rules.size()));

        assertThat(
            bucket.getLifecycleRules(), hasItems(
                matchers.toArray(new Matcher[0])
            )
        );

        // Delete bucket
        connection.delete(run.getBucket().getName());
    }

    @Test
    @Order(9)
    void createBucketWithDeleteLifecycleRule() throws Exception {
        String bucketId = "tu_lc_rule_delete_" + FriendlyId.createFriendlyId().toLowerCase();

        RunContext rc = runContext(bucketId);
        createBucketWithLifecycleRule(
            rc,
            Collections.singletonList(
                BucketLifecycleRule.builder()
                    .condition(BucketLifecycleRule.Condition.builder().age(1).build())
                    .action(
                        BucketLifecycleRule.Action.builder()
                            .type(BucketLifecycleRule.Action.Type.DELETE)
                            .build()
                    )
                    .build()
            )
        );
    }

    @Test
    @Order(10)
    void createBucketWithSetStorageClassLifecycleRule() throws Exception {
        String bucketId = "tu_lc_rule_class_" + FriendlyId.createFriendlyId().toLowerCase();

        RunContext rc = runContext(bucketId);
        createBucketWithLifecycleRule(
            rc,
            ImmutableList.of(
                BucketLifecycleRule.builder()
                    .condition(BucketLifecycleRule.Condition.builder().age(30).build())
                    .action(
                        BucketLifecycleRule.Action.builder()
                            .type(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS)
                            .value(io.kestra.plugin.gcp.gcs.models.StorageClass.NEARLINE.name())
                            .build()
                    )
                    .build(),

                BucketLifecycleRule.builder()
                    .condition(BucketLifecycleRule.Condition.builder().age(60).build())
                    .action(
                        BucketLifecycleRule.Action.builder()
                            .type(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS)
                            .value(io.kestra.plugin.gcp.gcs.models.StorageClass.COLDLINE.name())
                            .build()
                    )
                    .build(),

                BucketLifecycleRule.builder()
                    .condition(BucketLifecycleRule.Condition.builder().age(90).build())
                    .action(
                        BucketLifecycleRule.Action.builder()
                            .type(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS)
                            .value(io.kestra.plugin.gcp.gcs.models.StorageClass.ARCHIVE.name())
                            .build()
                    )
                    .build(),

                BucketLifecycleRule.builder()
                    .condition(BucketLifecycleRule.Condition.builder().age(1).build())
                    .action(
                        BucketLifecycleRule.Action.builder()
                            .type(BucketLifecycleRule.Action.Type.DELETE)
                            .build()
                    )
                    .build()
            )
        );
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

    // Custom match for LifecycleRule
    private Matcher<LifecycleRule> matchingLifecycleRuleOnTypeAndAge(LifecycleRule expected) {
        return new TypeSafeMatcher<LifecycleRule>() {
            @Override
            public void describeTo(Description description) {
                description.appendText(expected.toString());
            }

            @Override
            protected boolean matchesSafely(LifecycleRule actual) {
                return actual.getAction().getActionType().equals(expected.getAction().getActionType())
                    && actual.getCondition().getAge().equals(expected.getCondition().getAge())
                    && checkSetStorageRule(actual);
            }

            protected boolean checkSetStorageRule(LifecycleRule actual) {
                // If this is a set storage rule, we want to ensure that storage class value is the same
                if (!LifecycleRule.SetStorageClassLifecycleAction.TYPE.equals(actual.getAction().getActionType())) {
                    return true;
                }

                StorageClass actualStorageClass = ((LifecycleRule.SetStorageClassLifecycleAction) actual.getAction())
                    .getStorageClass();
                StorageClass expectedStorageClass = ((LifecycleRule.SetStorageClassLifecycleAction) expected.getAction())
                    .getStorageClass();

                return actualStorageClass != null && actualStorageClass.equals(expectedStorageClass);
            }
        };
    }
}
