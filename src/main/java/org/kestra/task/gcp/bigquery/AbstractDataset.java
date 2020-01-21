package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
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
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractDataset extends Task implements RunnableTask {
    @NotNull
    protected String name;
    protected String projectId;
    protected List<Acl> acl;
    protected Long defaultTableLifetime;
    protected String description;
    protected String friendlyName;
    protected String location;
    protected EncryptionConfiguration defaultEncryptionConfiguration;
    protected Long defaultPartitionExpirationMs;
    protected Map<String, String> labels;

    protected DatasetInfo datasetInfo(RunContext runContext) throws Exception {
        DatasetInfo.Builder builder = DatasetInfo.newBuilder(runContext.render(this.name));

        if (this.acl != null) {
            builder.setAcl(this.acl);
        }

        if (this.defaultTableLifetime != null) {
            builder.setDefaultTableLifetime(this.defaultTableLifetime);
        }

        if (this.description != null) {
            builder.setDescription(runContext.render(this.description));
        }

        if (this.friendlyName != null) {
            builder.setFriendlyName(runContext.render(this.friendlyName));
        }

        if (this.location != null) {
            builder.setLocation(runContext.render(this.location));
        }

        if (this.defaultEncryptionConfiguration != null) {
            builder.setDefaultEncryptionConfiguration(this.defaultEncryptionConfiguration);
        }

        if (this.defaultPartitionExpirationMs != null) {
            builder.setDefaultPartitionExpirationMs(this.defaultPartitionExpirationMs);
        }

        if (this.labels != null) {
            builder.setLabels(this.labels);
        }

        return builder.build();
    }

    protected Map<String, Object> outputs(DatasetInfo dataset) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("dataset", dataset.getDatasetId().getDataset())
            .put("project", dataset.getDatasetId().getProject());

        if (dataset.getFriendlyName() != null) {
            builder.put("friendlyName", dataset.getFriendlyName());
        }

        if (dataset.getDescription() != null) {
            builder.put("description", dataset.getDescription());
        }

        if (dataset.getLocation() != null) {
            builder.put("location", dataset.getLocation());
        }

        return builder.build();
    }
}
