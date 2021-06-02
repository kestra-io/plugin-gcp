package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
import io.kestra.plugin.gcp.bigquery.models.AccessControl;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;

import javax.validation.constraints.NotNull;
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
        title = "The dataset's user-defined id"
    )
    @PluginProperty(dynamic = true)
    protected String name;

    @Schema(
        title = "The dataset's access control configuration"
    )
    protected List<AccessControl> acl;

    @Schema(
        title = "The default lifetime of all tables in the dataset, in milliseconds",
        description = "The minimum value is" +
            " 3600000 milliseconds (one hour). Once this property is set, all newly-created tables in the" +
            " dataset will have an expirationTime property set to the creation time plus the value in this" +
            " property, and changing the value will only affect new tables, not existing ones. When the" +
            " expirationTime for a given table is reached, that table will be deleted automatically. If a" +
            " table's expirationTime is modified or removed before the table expires, or if you provide an" +
            " explicit expirationTime when creating a table, that value takes precedence over the default" +
            " expiration time indicated by this property. This property is experimental and might be" +
            " subject to change or removed."
    )
    protected Long defaultTableLifetime;

    @Schema(
        title = "Description",
        description = "A user-friendly description for the dataset."
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "A user-friendly name for the dataset"
    )
    @PluginProperty(dynamic = true)
    protected String friendlyName;

    @Schema(
        title = "The geographic location where the dataset should reside",
        description = "This property is experimental" +
            " and might be subject to change or removed." +
            " \n" +
            " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset" +
            "      Location</a>"
    )
    @PluginProperty(dynamic = true)
    protected String location;

    @Schema(
        title = "The default encryption key for all tables in the dataset",
        description = "Once this property is set, all" +
            " newly-created partitioned tables in the dataset will have encryption key set to this value," +
            " unless table creation request (or query) overrides the key."
    )
    protected EncryptionConfiguration defaultEncryptionConfiguration;

    @Schema(
        title = "[Optional] The default partition expiration time for all partitioned tables in the dataset, in milliseconds",
        description = " Once this property is set, all newly-created partitioned tables in the " +
            " dataset will has an expirationMs property in the timePartitioning settings set to this value." +
            " Changing the value only affect new tables, not existing ones. The storage in a partition will" +
            " have an expiration time of its partition time plus this value. Setting this property" +
            " overrides the use of defaultTableExpirationMs for partitioned tables: only one of" +
            " defaultTableExpirationMs and defaultPartitionExpirationMs will be used for any new" +
            " partitioned table. If you provide an explicit timePartitioning.expirationMs when creating or" +
            " updating a partitioned table, that value takes precedence over the default partition" +
            " expiration time indicated by this property. The value may be null."
    )
    protected Long defaultPartitionExpirationMs;

    @Schema(
        title = "The dataset's labels"
    )
    @PluginProperty(dynamic = true)
    protected Map<String, String> labels;

    protected DatasetInfo datasetInfo(RunContext runContext) throws Exception {
        DatasetInfo.Builder builder = DatasetInfo.newBuilder(runContext.render(this.name));

        if (this.acl != null) {
            builder.setAcl(mapAcls(this.acl));
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
            builder.setLabels(
                this.labels.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Rethrow.throwFunction(e -> runContext.render(e.getValue()))
                    ))
            );
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
            case IAM_MEMBER:
                return Acl.of(new Acl.IamMember(accessControl.getEntity().getValue()), Acl.Role.valueOf(accessControl.getRole().name()));
            default:
                return null;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @NotNull
        @Schema(
            title = "The dataset's user-defined id"
        )
        private String dataset;

        @NotNull
        @Schema(
            title = "The GCP project id"
        )
        private String project;

        @NotNull
        @Schema(
            title = "A user-friendly name for the dataset"
        )
        private String friendlyName;

        @NotNull
        @Schema(
            title = "A user-friendly description for the dataset"
        )
        private String description;

        @NotNull
        @Schema(
            title = "The geographic location where the dataset should reside",
            description = "This property is experimental" +
                " and might be subject to change or removed." +
                " \n" +
                " See <a href=\"https://cloud.google.com/bigquery/docs/reference/v2/datasets#location\">Dataset" +
                "      Location</a>"
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
