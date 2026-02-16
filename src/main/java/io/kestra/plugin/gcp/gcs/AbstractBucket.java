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
        title = "Bucket name",
        description = "Globally unique bucket ID"
    )
    protected Property<String> name;

    @Schema(
        title = "Requester pays",
        description = "If true, requester bears access egress charges"
    )
    protected Property<Boolean> requesterPays;

    @Schema(
        title = "Versioning enabled",
        description = "Enable object versioning; if true, previous versions are retained"
    )
    protected Property<Boolean> versioningEnabled;

    @Schema(
        title = "Website index page",
        description = "Served as directory index for static website hosting"
    )
    protected Property<String> indexPage;

    @Schema(
        title = "Website not-found page",
        description = "Custom 404 page for static website hosting"
    )
    protected Property<String> notFoundPage;

    @Schema(
        title = "Lifecycle rules",
        description = "List of lifecycle actions and conditions (age/delete/storage class supported)"
    )
    protected List<BucketLifecycleRule> lifecycleRules;

    @Schema(
        title = "Storage class",
        description = "Bucket storage class (cost/SLA); see Cloud Storage docs for values"
    )
    protected Property<StorageClass> storageClass;

    @Schema(
        title = "Location",
        description = "Bucket data location (region or dual-region)"
    )
    protected Property<String> location;

    @Schema(
        title = "CORS rules",
        description = "Cross-Origin Resource Sharing configuration"
    )
    protected List<Cors> cors;

    @Schema(
        title = "ACL",
        description = "Bucket-level access control list"
    )
    protected List<AccessControl> acl;

    @Schema(
        title = "Default object ACL",
        description = "Default ACL applied to new objects"
    )
    protected List<AccessControl> defaultAcl;

    @Schema(
        title = "Labels"
    )
    protected Property<Map<String, String>> labels;

    @Schema(
        title = "Default KMS key"
    )
    protected Property<String> defaultKmsKeyName;

    @Schema(
        title = "Default event-based hold",
        description = "If true, new objects are event-based held"
    )
    protected Property<Boolean> defaultEventBasedHold;

    @Schema(
        title = "Retention period (seconds)",
        description = "Bucket retention duration; can be reduced only if policy is unlocked"
    )
    protected Property<Long> retentionPeriod;

    @Schema(
        title = "IAM configuration",
        description = "Uniform bucket-level access and related settings"
    )
    protected IamConfiguration iamConfiguration;

    @Schema(
        title = "Logging",
        description = "Destination bucket and prefix for access logs"
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
