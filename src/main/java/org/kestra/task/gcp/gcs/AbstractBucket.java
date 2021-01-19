package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.LifecycleRule;
import com.google.cloud.storage.Cors;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.Rethrow;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBucket extends AbstractGcs implements RunnableTask<AbstractBucket.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    @PluginProperty(dynamic = true)
    protected String name;

    @Schema(
        title = "Whether the requester pays or not.",
        description = "Whether a user accessing the bucket or an object it contains should assume the transit " +
            " costs related to the access."
    )
    protected Boolean requesterPays;

    @Schema(
        title = "Whether versioning should be enabled for this bucket",
        description = "When set to true, versioning is " +
            " fully enabled."
    )
    protected Boolean versioningEnabled;

    @Schema(
        title = "The bucket's website index page",
        description = "Behaves as the bucket's directory index where missing " +
            " blobs are treated as potential directories."
    )
    protected String indexPage;

    @Schema(
        title = "The custom object to return when a requested resource is not found"
    )
    protected String notFoundPage;

    @Schema(
        title = "The bucket's lifecycle configuration",
        description = "This configuration is expressed as a number of lifecycle rules, consisting of an" +
            " action and a condition." +
            " \n" +
            " See <a href=\"https://cloud.google.com/storage/docs/lifecycle\">Object Lifecycle Management </a>" +
            " \n" +
            " Only the age condition is supported. Only the delete and SetStorageClass actions are supported"
    )
    protected List<BucketLifecycleRule> lifecycleRules;

    @Schema(
        title = "The bucket's storage class",
        description = "This defines how blobs in the bucket are stored and " +
            " determines the SLA and the cost of storage. A list of supported values is available <a" +
            " href=\"https://cloud.google.com/storage/docs/storage-classes\">here</a>."
    )
    protected StorageClass storageClass;

    @Schema(
        title = "The bucket's location",
        description = "Data for blobs in the bucket resides in physical storage within" +
            " this region. A list of supported values is available <a" +
            " href=\"https://cloud.google.com/storage/docs/bucket-locations\">here</a>."
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Schema(
        title = "The bucket's Cross-Origin Resource Sharing (CORS) configuration",
        description = " See <a href=\"https://cloud.google.com/storage/docs/cross-origin\">Cross-Origin Resource" +
            "Sharing (CORS)</a>"
    )
    protected List<Cors> cors;

    @Schema(
        title = "The bucket's access control configuration",
        description = " See <a" +
            " href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">" +
            "About Access Control Lists</a>"
    )
    protected List<AccessControl> acl;

    @Schema(
        title = "The default access control configuration",
        description = "The access control configuration to apply to bucket's blobs when no other" +
            " configuration is specified." +
            "\n" +
            " See <a" +
            "     href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">" +
            "About Access Control Lists</a>"
    )
    protected List<AccessControl> defaultAcl;

    @Schema(
        title = "The labels of this bucket"
    )
    @PluginProperty(dynamic = true)
    protected Map<String, String> labels;

    @Schema(
        title = "The default Cloud KMS key name for this bucket"
    )
    protected String defaultKmsKeyName;

    @Schema(
        title = "The default event-based hold for this bucket"
    )
    protected Boolean defaultEventBasedHold;

    @Schema(
        title = "Retention period",
        description = "If policy is not locked this value can be cleared, increased, and decreased. If policy is " +
            " locked the retention period can only be increased."
    )
    protected Long retentionPeriod;

    @Schema(
        title = "The Bucket's IAM Configuration",
        description = " See <a href=\"https://cloud.google.com/storage/docs/uniform-bucket-level-access\">uniform " +
            " bucket-level access</a>"
    )
    protected BucketInfo.IamConfiguration iamConfiguration;

    @Schema(
        title = "The bucket's logging configuration",
        description = "This configuration defines the destination bucket and optional name" +
            " prefix for the current bucket's logs."
    )
    protected BucketInfo.Logging logging;

    protected BucketInfo bucketInfo(RunContext runContext) throws Exception {
        BucketInfo.Builder builder = BucketInfo.newBuilder(runContext.render(this.name));

        if (this.requesterPays != null) {
            builder.setRequesterPays(this.requesterPays);
        }

        if (this.versioningEnabled != null) {
            builder.setVersioningEnabled(this.versioningEnabled);
        }

        if (this.indexPage != null) {
            builder.setIndexPage(this.indexPage);
        }

        if (this.notFoundPage != null) {
            builder.setNotFoundPage(this.notFoundPage);
        }

        if (this.lifecycleRules != null) {
            builder.setLifecycleRules(mapLifecycleRules(this.lifecycleRules));
        }

        if (this.storageClass != null) {
            builder.setStorageClass(com.google.cloud.storage.StorageClass.valueOf(this.storageClass.toString()));
        }

        if (this.location != null) {
            builder.setLocation(runContext.render(this.location));
        }

        if (this.cors != null) {
            builder.setCors(this.cors);
        }

        if (this.acl != null) {
            builder.setAcl(mapAcls(this.acl));
        }

        if (this.defaultAcl != null) {
            builder.setDefaultAcl(mapAcls(this.defaultAcl));
        }

        if (this.labels != null) {
            builder.setLabels(
                this.labels.entrySet().stream()
                    .collect(Collectors.toMap(
                        e -> e.getKey(),
                        Rethrow.throwFunction(e -> runContext.render(e.getValue()))
                    ))
            );
        }

        if (this.defaultKmsKeyName != null) {
            builder.setDefaultKmsKeyName(this.defaultKmsKeyName);
        }

        if (this.defaultEventBasedHold != null) {
            builder.setDefaultEventBasedHold(this.defaultEventBasedHold);
        }

        if (this.retentionPeriod != null) {
            builder.setRetentionPeriod(this.retentionPeriod);
        }

        if (this.iamConfiguration != null) {
            builder.setIamConfiguration(this.iamConfiguration);
        }

        if (this.logging != null) {
            builder.setLogging(this.logging);
        }

        return builder.build();
    }


    private List<Acl> mapAcls(List<AccessControl> accessControls) {
        if (accessControls == null) {
            return null;
        }

        List<Acl> acls = new ArrayList<>();
        for (AccessControl accessControl : accessControls) {
            acls.add(mapAcl(accessControl));
        }

        return acls;
    }

    private Acl mapAcl(AccessControl accessControl) {
        if (accessControl == null || accessControl.getEntity() == null || accessControl.getRole() == null) {
            return null;
        }

        switch (accessControl.getEntity().getType()) {
            case USER:
                return Acl.of(new Acl.User(accessControl.getEntity().getValue()), Acl.Role.valueOf(accessControl.getRole().name()));
            case GROUP:
                return Acl.of(new Acl.Group(accessControl.getEntity().getValue()), Acl.Role.valueOf(accessControl.getRole().name()));
            case DOMAIN:
                return Acl.of(new Acl.Domain(accessControl.getEntity().getValue()), Acl.Role.valueOf(accessControl.getRole().name()));
            default:
                return null;
        }
    }


    @SuppressWarnings("unused")
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class AccessControl {
        @NotNull
        @Schema(
            title = "The entity"
        )
        @PluginProperty(dynamic = true)
        private Entity entity;

        @SuppressWarnings("unused")
        @NoArgsConstructor
        @AllArgsConstructor
        @Builder
        @Getter
        public static class Entity {
            @NotNull
            @Schema(
                title = "The type of the entity (USER, GROUP or DOMAIN)"
            )
            @PluginProperty(dynamic = true)
            private Type type;

            @NotNull
            @Schema(
                title = "The value for the entity (ex : user email if the type is USER ...)"
            )
            @PluginProperty(dynamic = true)
            private String value;

            @SuppressWarnings("unused")
            public enum Type {
                DOMAIN,
                GROUP,
                USER
            }
        }

        @NotNull
        @Schema(
            title = "The role to assign to the entity"
        )
        @PluginProperty(dynamic = true)
        private Role role;

        @SuppressWarnings("unused")
        public enum Role {
            READER,
            WRITER,
            OWNER
        }
    }

    public List<LifecycleRule> mapLifecycleRules(List<BucketLifecycleRule> bucketLifecycleRules) {
        if (bucketLifecycleRules == null) {
            return null;
        }

        List<LifecycleRule> rules = new ArrayList<>();
        for (BucketLifecycleRule bucketLifecycleRule : bucketLifecycleRules) {
            rules.add(mapLifecycleRule(bucketLifecycleRule));
        }

        return rules;
    }

    public LifecycleRule mapLifecycleRule(BucketLifecycleRule bucketLifecycleRule) {
        if (bucketLifecycleRule == null || bucketLifecycleRule.getCondition() == null || bucketLifecycleRule.getAction() == null) {
            return null;
        }

        switch (bucketLifecycleRule.getAction().getType()) {
            case DELETE:
                return BucketLifecycleRule.DeleteAction.builder()
                    .build()
                    .convert(bucketLifecycleRule.getCondition());
            case SET_STORAGE_CLASS:
                return BucketLifecycleRule.SetStorageAction.builder()
                    .storageClass(StorageClass.valueOf((String) bucketLifecycleRule.getAction().getValue()))
                    .build()
                    .convert(bucketLifecycleRule.getCondition());
            default:
                return null;

        }
    }

    @SuppressWarnings("unused")
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class BucketLifecycleRule {
        @NotNull
        @Schema(
            title = "The condition"
        )
        @PluginProperty(dynamic = true)
        private AbstractBucket.BucketLifecycleRule.Condition condition;

        @NotNull
        @Schema(
            title = "The action to take when a lifecycle condition is met"
        )
        @PluginProperty(dynamic = true)
        private AbstractBucket.BucketLifecycleRule.Action action;

        @SuppressWarnings("unused")
        @NoArgsConstructor
        @AllArgsConstructor
        @Builder
        @Getter
        public static class Condition {
            @NotNull
            @Schema(
                title = "The Age condition is satisfied when an object reaches the specified age (in days). Age is measured from the object's creation time."
            )
            @PluginProperty(dynamic = true)
            private Integer age;
        }

        @SuppressWarnings("unused")
        @NoArgsConstructor
        @AllArgsConstructor
        @Builder
        @Getter
        public static class Action {
            @NotNull
            @Schema(
                title = "The type of the action (DELETE ...)"
            )
            @PluginProperty(dynamic = true)
            private AbstractBucket.BucketLifecycleRule.Action.Type type;

            @Schema(
                title = "The value for the action (if any)"
            )
            @PluginProperty(dynamic = true)
            private String value;

            @SuppressWarnings("unused")
            public enum Type {
                DELETE,
                SET_STORAGE_CLASS
            }
        }


        @SuppressWarnings("unused")
        @SuperBuilder
        @NoArgsConstructor
        @Getter
        public static class DeleteAction implements LifecycleAction {
            @Override
            public LifecycleRule convert(BucketLifecycleRule.Condition condition) {
                return new BucketInfo.LifecycleRule(
                    LifecycleRule.LifecycleAction.newDeleteAction(),
                    LifecycleRule.LifecycleCondition.newBuilder().setAge(condition.getAge()).build());
            }
        }

        @SuppressWarnings("unused")
        @SuperBuilder
        @AllArgsConstructor
        @NoArgsConstructor
        @Getter
        public static class SetStorageAction implements LifecycleAction {
            @NotNull
            @Schema(
                title = "The storage class (standard, nearline, coldline ...)"
            )
            @PluginProperty(dynamic = true)
            private AbstractBucket.StorageClass storageClass;

            @Override
            public LifecycleRule convert(BucketLifecycleRule.Condition condition) {
                return new BucketInfo.LifecycleRule(
                    LifecycleRule.LifecycleAction.newSetStorageClassAction(com.google.cloud.storage.StorageClass.valueOf(this.storageClass.name())),
                    LifecycleRule.LifecycleCondition.newBuilder().setAge(condition.getAge()).build());
            }
        }

        public interface LifecycleAction {
            BucketInfo.LifecycleRule convert(BucketLifecycleRule.Condition condition);
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket's unique name"
        )
        private String bucket;

        @Schema(
            title = "The bucket's URI."
        )
        private URI bucketUri;

        @Schema(
            title = "The bucket's location"
        )
        private String location;

        @Schema(
            title = "The bucket's website index page."
        )
        private String indexPage;

        @Schema(
            title = "The custom object to return when a requested resource is not found."
        )
        private String notFoundPage;

        public static Output of(Bucket bucket) throws URISyntaxException {
            return Output.builder()
                .bucket(bucket.getName())
                .bucketUri(new URI("gs://" + bucket.getName()))
                .location(bucket.getLocation())
                .indexPage(bucket.getIndexPage())
                .notFoundPage(bucket.getNotFoundPage())
                .build();
        }
    }

    public enum StorageClass {
        REGIONAL,
        MULTI_REGIONAL,
        NEARLINE,
        COLDLINE,
        STANDARD,
        ARCHIVE,
        DURABLE_REDUCED_AVAILABILITY
    }
}
