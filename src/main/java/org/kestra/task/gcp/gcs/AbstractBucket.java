package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Cors;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractBucket extends Task implements RunnableTask<AbstractBucket.Output> {
    @NotNull
    @InputProperty(
        description = "Bucket's unique name",
        dynamic = true
    )
    protected String name;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    protected String projectId;

    @InputProperty(
        description = "Whether the requester pays or not.",
        body = "Whether a user accessing the bucket or an object it contains should assume the transit\n" +
            " costs related to the access."
    )
    protected Boolean requesterPays;

    @InputProperty(
        description = "Whether versioning should be enabled for this bucket",
        body = "When set to true, versioning is\n" +
            " fully enabled."
    )
    protected Boolean versioningEnabled;

    @InputProperty(
        description = "The bucket's website index page",
        body = "Behaves as the bucket's directory index where missing\n" +
            " blobs are treated as potential directories."
    )
    protected String indexPage;

    @InputProperty(
        description = "The custom object to return when a requested resource is not found"
    )
    protected String notFoundPage;

    @InputProperty(
        description = "The bucket's lifecycle configuration",
        body = "This configuration is expressed as a number of lifecycle rules, consisting of an\n" +
            " action and a condition.\n" +
            " \n" +
            " See <a href=\"https://cloud.google.com/storage/docs/lifecycle\">Object Lifecycle\n" +
            " Management</a>"
    )
    protected List<BucketInfo.LifecycleRule> lifecycleRules;

    @InputProperty(
        description = "The bucket's storage class",
        body = "This defines how blobs in the bucket are stored and\n" +
            " determines the SLA and the cost of storage. A list of supported values is available <a\n" +
            " href=\"https://cloud.google.com/storage/docs/storage-classes\">here</a>."
    )
    protected StorageClass storageClass;

    @InputProperty(
        description = "The bucket's location",
        body = "Data for blobs in the bucket resides in physical storage within\n" +
            " this region. A list of supported values is available <a\n" +
            " href=\"https://cloud.google.com/storage/docs/bucket-locations\">here</a>.",
        dynamic = true
    )
    protected String location;

    @InputProperty(
        description = "The bucket's Cross-Origin Resource Sharing (CORS) configuration",
        body = " See <a href=\"https://cloud.google.com/storage/docs/cross-origin\">Cross-Origin Resource\n" +
            " Sharing (CORS)</a>"
    )
    protected List<Cors> cors;

    @InputProperty(
        description = "The bucket's access control configuration",
        body = " See <a\n" +
            " href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">\n" +
            " About Access Control Lists</a>"
    )
    protected List<Acl> acl;

    @InputProperty(
        description = "The default access control configuration",
        body = "The access control configuration to apply to bucket's blobs when no other\n" +
            " configuration is specified.\n" +
            "\n" +
            " Ssee <a\n" +
            "     href=\"https://cloud.google.com/storage/docs/access-control#About-Access-Control-Lists\">\n" +
            "     About Access Control Lists</a>"
    )
    protected List<Acl> defaultAcl;

    @InputProperty(
        description = "The labels of this bucket"
    )
    protected Map<String, String> labels;

    @InputProperty(
        description = "The default Cloud KMS key name for this bucket"
    )
    protected String defaultKmsKeyName;

    @InputProperty(
        description = "The default event-based hold for this bucket"
    )
    protected Boolean defaultEventBasedHold;

    @InputProperty(
        description = "Retention period",
        body = "If policy is not locked this value can be cleared, increased, and decreased. If policy is\n" +
            " locked the retention period can only be increased."
    )
    protected Long retentionPeriod;

    @InputProperty(
        description = "The Bucket's IAM Configuration",
        body = " See <a href=\"https://cloud.google.com/storage/docs/uniform-bucket-level-access\">uniform\n" +
            "      bucket-level access</a>"
    )
    protected BucketInfo.IamConfiguration iamConfiguration;

    @InputProperty(
        description = "The bucket's logging configuration",
        body = "This configuration defines the destination bucket and optional name\n" +
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
            builder.setAcl(this.acl);
        }

        if (this.defaultAcl != null) {
            builder.setDefaultAcl(this.defaultAcl);
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

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The bucket's unique name"
        )
        private String bucket;

        @OutputProperty(
            description = "The bucket's URI."
        )
        private URI bucketUri;

        @OutputProperty(
            description = "The bucket's location"
        )
        private String location;

        @OutputProperty(
            description = "The bucket's website index page."
        )
        private String indexPage;

        @OutputProperty(
            description = "The custom object to return when a requested resource is not found."
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
