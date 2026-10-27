package io.kestra.plugin.gcp.gcs;

import java.util.Collections;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.devskiller.friendly_id.FriendlyId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.LifecycleRule;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.AbstractFlociGcpTest;
import io.kestra.plugin.gcp.gcs.models.BucketLifecycleRule;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlociBucketTest extends AbstractFlociGcpTest {
    private static final String RANDOM_ID = "tu_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String RANDOM_ID_2 = "tu_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String LIFECYCLE_DELETE_BUCKET = "tu_lc_rule_delete_" + FriendlyId.createFriendlyId().toLowerCase();
    private static final String LIFECYCLE_STORAGE_CLASS_BUCKET = "tu_lc_rule_class_" + FriendlyId.createFriendlyId().toLowerCase();

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
            .id(FlociBucketTest.class.getSimpleName())
            .type(CreateBucket.class.getName())
            .name(Property.ofExpression("{{bucket}}"))
            .projectId(Property.ofValue(project));
    }

    @AfterAll
    static void cleanUpBuckets() {
        if (storage == null) {
            return;
        }

        for (String bucketName : List.of(
            RANDOM_ID,
            RANDOM_ID_2,
            LIFECYCLE_DELETE_BUCKET,
            LIFECYCLE_STORAGE_CLASS_BUCKET
        )) {
            if (storage.get(bucketName) != null) {
                storage.delete(bucketName);
            }
        }
    }

    @Test
    @Order(1)
    void create() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var task = emulatorScope.spy(createBuilder().build());
            var run = task.run(runContext());

            assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
        }
    }

    @Test
    @Order(2)
    void createException() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var task = emulatorScope.spy(createBuilder().build());

            assertThrows(RuntimeException.class, () ->
            {
                task.run(runContext());
            });
        }
    }

    @Test
    @Order(3)
    void createNoException() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var task = emulatorScope.spy(
                createBuilder()
                    .indexPage(Property.ofValue("createUpdate"))
                    .ifExists(Property.ofValue(CreateBucket.IfExists.SKIP))
                    .build()
            );

            var run = task.run(runContext());

            assertThat(run.getBucket().getName(), is(runContext().getVariables().get("bucket")));
        }
    }

    @Test
    @Order(4)
    void iamPolicy() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var builder = CreateBucketIamPolicy.builder()
                .id(UpdateBucket.class.getSimpleName())
                .type(CreateBucket.class.getName())
                .name(Property.ofExpression("{{bucket}}"))
                .projectId(Property.ofExpression("{{project}}"))
                .member(Property.ofValue("domain:kestra.io"))
                .role(Property.ofValue("roles/storage.objectViewer"));

            var run = emulatorScope.spy(builder.build()).run(runContext());
            assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));

            assertThrows(Exception.class, () ->
            {
                emulatorScope.spy(builder.ifExists(Property.ofValue(CreateBucketIamPolicy.IfExists.ERROR)).build())
                    .run(runContext());
            });

            run = emulatorScope.spy(builder.ifExists(Property.ofValue(CreateBucketIamPolicy.IfExists.SKIP)).build())
                .run(runContext());
            assertThat(run.getBucket(), is(runContext().getVariables().get("bucket")));
        }
    }

    @Test
    @Order(5)
    void delete() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            // The original ACL test created this second bucket. Keep delete independent of that cloud-only case.
            if (storage.get(RANDOM_ID_2) == null) {
                storage.create(BucketInfo.of(RANDOM_ID_2));
            }

            var runContext = runContext();

            var task = emulatorScope.spy(
                DeleteBucket.builder()
                    .id(FlociBucketTest.class.getSimpleName())
                    .type(DeleteBucket.class.getName())
                    .name(Property.ofExpression("{{bucket}}"))
                    .projectId(Property.ofExpression("{{project}}"))
                    .build()
            );

            var run = task.run(runContext);
            assertThat(run.getBucket(), is(runContext.getVariables().get("bucket")));

            runContext = runContext(RANDOM_ID_2);

            task = emulatorScope.spy(
                DeleteBucket.builder()
                    .id(FlociBucketTest.class.getSimpleName())
                    .type(DeleteBucket.class.getName())
                    .name(Property.ofExpression("{{bucket}}"))
                    .projectId(Property.ofExpression("{{project}}"))
                    .build()
            );

            run = task.run(runContext);
            assertThat(run.getBucket(), is(runContext.getVariables().get("bucket")));
        }
    }

    @SuppressWarnings("unchecked")
    private void createBucketWithLifecycleRule(
        GcsTestConnections.Scope connections,
        RunContext rc,
        String bucketId,
        List<BucketLifecycleRule> rules) throws Exception {
        var task = connections.spy(
            createBuilder()
                .lifecycleRules(rules)
                .ifExists(Property.ofValue(CreateBucket.IfExists.ERROR))
                .build()
        );

        try {
            var run = task.run(rc);

            assertThat(run.getBucket().getName(), is(rc.getVariables().get("bucket")));

            var bucket = storage.get(run.getBucket().getName());

            assertThat(null, not(bucket));

            assertThat(bucket.getLifecycleRules(), notNullValue());
            assertThat(bucket.getLifecycleRules().size(), is(rules.size()));

            var matchers = task
                .getLifecycleRules()
                .stream()
                .map(rule ->
                {
                    try {
                        return matchingLifecycleRuleOnTypeAndAge(rule.convert(rc));
                    } catch (IllegalVariableEvaluationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

            assertThat(matchers.size(), is(rules.size()));

            assertThat(
                bucket.getLifecycleRules(), hasItems(
                    matchers.toArray(new Matcher[0])
                )
            );
        } finally {
            if (storage.get(bucketId) != null) {
                storage.delete(bucketId);
            }
        }
    }

    @Test
    @Order(6)
    void createBucketWithDeleteLifecycleRule() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var rc = runContext(LIFECYCLE_DELETE_BUCKET);
            createBucketWithLifecycleRule(
                emulatorScope,
                rc,
                LIFECYCLE_DELETE_BUCKET,
                Collections.singletonList(
                    BucketLifecycleRule.builder()
                        .condition(BucketLifecycleRule.Condition.builder().age(Property.ofValue(1)).build())
                        .action(
                            BucketLifecycleRule.Action.builder()
                                .type(Property.ofValue(BucketLifecycleRule.Action.Type.DELETE))
                                .build()
                        )
                        .build()
                )
            );
        }
    }

    @Test
    @Order(7)
    void createBucketWithSetStorageClassLifecycleRule() throws Exception {
        try (GcsTestConnections.Scope emulatorScope = GcsTestConnections.useFloci(FLOCI_GCP.getEndpoint())) {
            var rc = runContext(LIFECYCLE_STORAGE_CLASS_BUCKET);
            createBucketWithLifecycleRule(
                emulatorScope,
                rc,
                LIFECYCLE_STORAGE_CLASS_BUCKET,
                List.of(
                    BucketLifecycleRule.builder()
                        .condition(BucketLifecycleRule.Condition.builder().age(Property.ofValue(30)).build())
                        .action(
                            BucketLifecycleRule.Action.builder()
                                .type(Property.ofValue(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS))
                                .value(Property.ofValue(io.kestra.plugin.gcp.gcs.models.StorageClass.NEARLINE.name()))
                                .build()
                        )
                        .build(),

                    BucketLifecycleRule.builder()
                        .condition(BucketLifecycleRule.Condition.builder().age(Property.ofValue(60)).build())
                        .action(
                            BucketLifecycleRule.Action.builder()
                                .type(Property.ofValue(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS))
                                .value(Property.ofValue(io.kestra.plugin.gcp.gcs.models.StorageClass.COLDLINE.name()))
                                .build()
                        )
                        .build(),

                    BucketLifecycleRule.builder()
                        .condition(BucketLifecycleRule.Condition.builder().age(Property.ofValue(90)).build())
                        .action(
                            BucketLifecycleRule.Action.builder()
                                .type(Property.ofValue(BucketLifecycleRule.Action.Type.SET_STORAGE_CLASS))
                                .value(Property.ofValue(io.kestra.plugin.gcp.gcs.models.StorageClass.ARCHIVE.name()))
                                .build()
                        )
                        .build(),

                    BucketLifecycleRule.builder()
                        .condition(BucketLifecycleRule.Condition.builder().age(Property.ofValue(1)).build())
                        .action(
                            BucketLifecycleRule.Action.builder()
                                .type(Property.ofValue(BucketLifecycleRule.Action.Type.DELETE))
                                .build()
                        )
                        .build()
                )
            );
        }
    }

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
                if (!LifecycleRule.SetStorageClassLifecycleAction.TYPE.equals(actual.getAction().getActionType())) {
                    return true;
                }

                var actualStorageClass = ((LifecycleRule.SetStorageClassLifecycleAction) actual.getAction()).getStorageClass();
                var expectedStorageClass = ((LifecycleRule.SetStorageClassLifecycleAction) expected.getAction()).getStorageClass();

                return actualStorageClass != null && actualStorageClass.equals(expectedStorageClass);
            }
        };
    }
}
