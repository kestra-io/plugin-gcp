package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
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
abstract public class AbstractBucket extends Task implements RunnableTask {
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
            builder.setStorageClass(this.storageClass);
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


    protected Map<String, Object> outputs(Bucket bucket) throws URISyntaxException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("bucket", bucket.getName())
            .put("bucketUri", new URI("gs://" + bucket.getName()));

        if (bucket.getLocation() != null) {
            builder.put("location", bucket.getLocation());
        }

        if (bucket.getIndexPage() != null) {
            builder.put("indexPage", bucket.getIndexPage());
        }

        if (bucket.getNotFoundPage() != null) {
            builder.put("notFoundPage", bucket.getNotFoundPage());
        }


        return builder.build();
    }



}
