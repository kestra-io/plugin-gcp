package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Cors;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBucket extends Task implements RunnableTask<AbstractBucket.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    @PluginProperty(dynamic = true)
    protected String name;

    @Schema(
        title = "The GCP project id"
    )
    @PluginProperty(dynamic = true)
    protected String projectId;

    @Schema(
        title = "Whether the requester pays or not.",
        description = "Whether a user accessing the bucket or an object it contains should assume the transit\n" +
            " costs related to the access."
    )
    protected Boolean requesterPays;

    @Schema(
        title = "Whether versioning should be enabled for this bucket",
        description = "When set to true, versioning is\n" +
            " fully enabled."
    )
    protected Boolean versioningEnabled;

    @Schema(
        title = "The bucket's website index page",
        description = "Behaves as the bucket's directory index where missing\n" +
            " blobs are treated as potential directories."
    )
    protected String indexPage;

    @Schema(
        title = "The custom object to return when a requested resource is not found"
    )
    protected String notFoundPage;

    @Schema(
        title = "The bucket's lifecycle configuration",
        description = "This configuration is expressed as a number of lifecycle rules, consisting of an\n" +
            " action and a condition.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/storage/docs/lifecycle\">Object Lifecycle\n" +
            " Management</a>"
    )
    protected List<BucketInfo.LifecycleRule> lifecycleRules;

    @Schema(
        title = "The bucket's storage class",
        description = "This defines how blobs in the bucket are stored and\n" +
            " determines the SLA and the cost of storage. A list of supported values is available <a\n" +
            " href=\"https://cloud.google.com/storage/docs/storage-classes\">here</a>."
    )
    protected StorageClass storageClass;

    @Schema(
        title = "The bucket's location",
        description = "Data for blobs in the bucket resides in physical storage within\n" +
            " this region. A list of supported values is available <a\n" +
            " href=\"https://cloud.google.com/storage/docs/bucket-locations\">here</a>."
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Schema(
        title = "The bucket's Cross-Origin Resource Sharing (CORS) configuration",
        description = " See <a href=\"https://cloud.google.com/storage/docs/cross-origin\">Cross-Origin Resource\n" +
            " Sharing (CORS)</a>"
    )
    protected List<Cors> cors;

    @Schema(
        title = "The bucket's access control configuration",
        description = " See <a\n" +
            " href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">\n" +
            " About Access Control Lists</a>"
    )
    protected List<AccessControl> acl;

    @Schema(
        title = "The default access control configuration",
        description = "The access control configuration to apply to bucket's blobs when no other\n" +
            " configuration is specified.\n" +
            "\n" +
            " Ssee <a\n" +
            "     href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">\n" +
            "     About Access Control Lists</a>"
    )
    protected List<AccessControl> defaultAcl;

    @Schema(
        title = "The labels of this bucket"
    )
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
        description = "If policy is not locked this value can be cleared, increased, and decreased. If policy is\n" +
            " locked the retention period can only be increased."
    )
    protected Long retentionPeriod;

    @Schema(
        title = "The Bucket's IAM Configuration",
        description = " See <a href=\"https://cloud.google.com/storage/docs/uniform-bucket-level-access\">uniform\n" +
            "      bucket-level access</a>"
    )
    protected BucketInfo.IamConfiguration iamConfiguration;

    @Schema(
        title = "The bucket's logging configuration",
        description = "This configuration defines the destination bucket and optional name\n" +
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
            builder.setLifecycleRules(this.lifecycleRules);
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
            builder.setLabels(this.labels);
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
        STANDARD
    }
}
