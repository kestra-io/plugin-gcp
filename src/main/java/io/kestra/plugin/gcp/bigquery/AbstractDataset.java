package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.gcp.bigquery.models.AccessControl;
import io.kestra.plugin.gcp.bigquery.models.Entity;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;

import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractDataset extends AbstractBigquery implements RunnableTask<AbstractDataset.Output> {
    @NotNull
    @Schema(
        title = "Dataset ID",
        description = "Required dataset identifier"
    )
    protected Property<String> name;

    @Schema(
        title = "Access controls",
        description = "List of dataset ACL entries (see AccessControl)"
    )
    @PluginProperty
    protected List<AccessControl> acl;

    @Schema(
        title = "Default table expiration (ms)",
        description = "Minimum 3,600,000 ms (1h). Applies to new tables only; existing tables keep their own expiration. Experimental."
    )
    protected Property<Long> defaultTableLifetime;

    @Schema(
        title = "Dataset description",
        description = "User-friendly description; supports templating"
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "Dataset display name"
    )
    protected Property<String> friendlyName;

    @Schema(
        title = "Dataset location",
        description = "Optional BigQuery location; experimental and may change. See BigQuery dataset location docs."
    )
    protected Property<String> location;

    @Schema(
        title = "Default table encryption key",
        description = "CMEK applied to newly created tables unless overridden"
    )
    @PluginProperty
    protected EncryptionConfiguration defaultEncryptionConfiguration;

    @Schema(
        title = "Default partition expiration (ms)",
        description = "Applied to new partitioned tables only; overrides defaultTableLifetime for partitions"
    )
    protected Property<Long> defaultPartitionExpirationMs;

    @Schema(
        title = "Dataset labels"
    )
    protected Property<Map<String, String>> labels;

    protected DatasetInfo datasetInfo(RunContext runContext) throws Exception {
        DatasetInfo.Builder builder = DatasetInfo.newBuilder(runContext.render(this.name).as(String.class).orElseThrow());

        if (this.acl != null) {
            builder.setAcl(mapAcls(this.acl, runContext));
        }

        if (this.defaultTableLifetime != null) {
            builder.setDefaultTableLifetime(runContext.render(this.defaultTableLifetime).as(Long.class).orElseThrow());
        }

        if (this.description != null) {
            builder.setDescription(runContext.render(this.description));
        }

        if (this.friendlyName != null) {
            builder.setFriendlyName(runContext.render(this.friendlyName).as(String.class).orElseThrow());
        }

        if (this.location != null) {
            builder.setLocation(runContext.render(this.location).as(String.class).orElseThrow());
        }

        if (this.defaultEncryptionConfiguration != null) {
            builder.setDefaultEncryptionConfiguration(this.defaultEncryptionConfiguration);
        }

        if (this.defaultPartitionExpirationMs != null) {
            builder.setDefaultPartitionExpirationMs(runContext.render(this.defaultPartitionExpirationMs).as(Long.class).orElseThrow());
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

        return builder.build();
    }

    private List<Acl> mapAcls(List<AccessControl> accessControls, RunContext runContext) throws IllegalVariableEvaluationException {
        if (accessControls == null) {
            return null;
        }

        List<Acl> acls = new ArrayList<>();
        for (AccessControl accessControl : accessControls) {
            acls.add(mapAcl(accessControl, runContext));
        }

        return acls;
    }

    private Acl mapAcl(AccessControl accessControl, RunContext runContext) throws IllegalVariableEvaluationException {
        if (accessControl == null || accessControl.getEntity() == null || accessControl.getRole() == null) {
            return null;
        }

        var renderedValue = runContext.render(accessControl.getEntity().getValue()).as(String.class);
        var renderedRole = runContext.render(accessControl.getRole()).as(AccessControl.Role.class);

        return switch (runContext.render(accessControl.getEntity().getType()).as(Entity.Type.class).orElseThrow()) {
            case USER -> Acl.of(new Acl.User(renderedValue.get()), Acl.Role.valueOf(renderedRole.get().name()));
            case GROUP -> Acl.of(new Acl.Group(renderedValue.get()), Acl.Role.valueOf(renderedRole.get().name()));
            case DOMAIN -> Acl.of(new Acl.Domain(renderedValue.get()), Acl.Role.valueOf(renderedRole.get().name()));
            case IAM_MEMBER ->
                Acl.of(new Acl.IamMember(renderedValue.get()), Acl.Role.valueOf(renderedRole.get().name()));
            default -> null;
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @NotNull
        @Schema(
            title = "Dataset ID"
        )
        private String dataset;

        @NotNull
        @Schema(
            title = "GCP project ID"
        )
        private String project;

        @NotNull
        @Schema(
            title = "Dataset display name"
        )
        private String friendlyName;

        @NotNull
        @Schema(
            title = "Dataset description"
        )
        private String description;

        @NotNull
        @Schema(
            title = "Dataset location",
            description = "Optional BigQuery location; experimental and may change"
        )
        private String location;

        public static Output of(DatasetInfo dataset) {
            return Output.builder()
                .dataset(dataset.getDatasetId().getDataset())
                .project(dataset.getDatasetId().getProject())
                .friendlyName(dataset.getFriendlyName())
                .description(dataset.getDescription())
                .location(dataset.getLocation())
                .build();
        }
    }
}
