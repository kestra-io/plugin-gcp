package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BucketInfo;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.gcp.gcs.models.*;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBucket extends AbstractGcs implements RunnableTask<AbstractBucket.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    protected Property<String> name;

    @Schema(
        title = "Whether the requester pays or not.",
        description = "Whether a user accessing the bucket or an object it contains should assume the transit " +
            " costs related to the access."
    )
    protected Property<Boolean> requesterPays;

    @Schema(
        title = "Whether versioning should be enabled for this bucket",
        description = "When set to true, versioning is " +
            " fully enabled."
    )
    protected Property<Boolean> versioningEnabled;

    @Schema(
        title = "The bucket's website index page",
        description = "Behaves as the bucket's directory index where missing " +
            " blobs are treated as potential directories."
    )
    protected Property<String> indexPage;

    @Schema(
        title = "The custom object to return when a requested resource is not found"
    )
    protected Property<String> notFoundPage;

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
    protected Property<StorageClass> storageClass;

    @Schema(
        title = "The bucket's location",
        description = "Data for blobs in the bucket resides in physical storage within" +
            " this region. A list of supported values is available <a" +
            " href=\"https://cloud.google.com/storage/docs/bucket-locations\">here</a>."
    )
    protected Property<String> location;

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
    protected Property<Map<String, String>> labels;

    @Schema(
        title = "The default Cloud KMS key name for this bucket"
    )
    protected Property<String> defaultKmsKeyName;

    @Schema(
        title = "The default event-based hold for this bucket"
    )
    protected Property<Boolean> defaultEventBasedHold;

    @Schema(
        title = "Retention period",
        description = "If policy is not locked this value can be cleared, increased, and decreased. If policy is " +
            " locked the retention period can only be increased."
    )
    protected Property<Long> retentionPeriod;

    @Schema(
        title = "The Bucket's IAM Configuration",
        description = " See <a href=\"https://cloud.google.com/storage/docs/uniform-bucket-level-access\">uniform " +
            " bucket-level access</a>"
    )
    protected IamConfiguration iamConfiguration;

    @Schema(
        title = "The bucket's logging configuration",
        description = "This configuration defines the destination bucket and optional name" +
            " prefix for the current bucket's logs."
    )
    protected Logging logging;

    protected BucketInfo bucketInfo(RunContext runContext) throws Exception {
        BucketInfo.Builder builder = BucketInfo.newBuilder(runContext.render(this.name).as(String.class).orElseThrow());

        if (this.requesterPays != null) {
            builder.setRequesterPays(runContext.render(this.requesterPays).as(Boolean.class).orElseThrow());
        }

        if (this.versioningEnabled != null) {
            builder.setVersioningEnabled(runContext.render(this.versioningEnabled).as(Boolean.class).orElseThrow());
        }

        if (this.indexPage != null) {
            builder.setIndexPage(runContext.render(this.indexPage).as(String.class).orElseThrow());
        }

        if (this.notFoundPage != null) {
            builder.setNotFoundPage(runContext.render(this.notFoundPage).as(String.class).orElseThrow());
        }

        if (this.lifecycleRules != null) {
            builder.setLifecycleRules(BucketLifecycleRule.convert(this.lifecycleRules, runContext));
        }

        if (this.storageClass != null) {
            builder.setStorageClass(com.google.cloud.storage.StorageClass.valueOf(runContext.render(this.storageClass).as(StorageClass.class).orElseThrow().toString()));
        }

        if (this.location != null) {
            builder.setLocation(runContext.render(this.location).as(String.class).orElseThrow());
        }

        if (this.cors != null) {
            builder.setCors(Cors.convert(this.cors));
        }

        if (this.acl != null) {
            builder.setAcl(AccessControl.convert(this.acl, runContext));
        }

        if (this.defaultAcl != null) {
            builder.setDefaultAcl(AccessControl.convert(this.defaultAcl, runContext));
        }

        if (this.labels != null) {
            builder.setLabels(
                runContext.render(this.labels).asMap(String.class, String.class).entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Rethrow.throwFunction(e -> runContext.render(e.getValue()))
                    ))
            );
        }

        if (this.defaultKmsKeyName != null) {
            builder.setDefaultKmsKeyName(runContext.render(this.defaultKmsKeyName).as(String.class).orElseThrow());
        }

        if (this.defaultEventBasedHold != null) {
            builder.setDefaultEventBasedHold(runContext.render(this.defaultEventBasedHold).as(Boolean.class).orElseThrow());
        }

        if (this.retentionPeriod != null) {
            builder.setRetentionPeriod(runContext.render(this.retentionPeriod).as(Long.class).orElseThrow());
        }

        if (this.iamConfiguration != null) {
            builder.setIamConfiguration(this.iamConfiguration.convert());
        }

        if (this.logging != null) {
            builder.setLogging(this.logging.convert());
        }

        return builder.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket's info"
        )
        private Bucket bucket;

        @Schema(
            title = "If the bucket was updated."
        )
        @Builder.Default
        private Boolean updated = false;

        @Schema(
            title = "If the bucket was created."
        )
        @Builder.Default
        private Boolean created = false;
    }
}
