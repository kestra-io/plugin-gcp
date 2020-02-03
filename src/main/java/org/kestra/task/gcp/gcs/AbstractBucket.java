package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.*;
import lombok.*;
import lombok.experimental.SuperBuilder;
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
    protected String name;
    protected String projectId;
    protected Boolean requesterPays;
    protected Boolean versioningEnabled;
    protected String indexPage;
    protected String notFoundPage;
    protected List<BucketInfo.LifecycleRule> lifecycleRules;
    protected StorageClass storageClass;
    protected String location;
    protected List<Cors> cors;
    protected List<Acl> acl;
    protected List<Acl> defaultAcl;
    protected Map<String, String> labels;
    protected String defaultKmsKeyName;
    protected Boolean defaultEventBasedHold;
    protected Long retentionPeriod;
    protected BucketInfo.IamConfiguration iamConfiguration;
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
        private String bucket;
        private URI bucketUri;
        private String location;
        private String indexPage;
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
