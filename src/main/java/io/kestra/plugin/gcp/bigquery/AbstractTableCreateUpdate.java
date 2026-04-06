package io.kestra.plugin.gcp.bigquery;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.TableInfo;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.gcp.bigquery.models.EncryptionConfiguration;
import io.kestra.plugin.gcp.bigquery.models.TableDefinition;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractTableCreateUpdate extends AbstractTable {
    @Schema(
        title = "Table definition",
        description = "Required table definition (standard, external, view, etc.)"
    )
    @PluginProperty(group = "advanced")
    protected TableDefinition tableDefinition;

    @Schema(
        title = "Table display name"
    )
    @PluginProperty(group = "advanced")
    protected Property<String> friendlyName;

    @Schema(
        title = "Table description"
    )
    @PluginProperty(dynamic = true, group = "advanced")
    protected String description;

    @Schema(
        title = "Table labels"
    )
    Property<Map<String, String>> labels;

    @Schema(
        title = "Require partition filter",
        description = "If true, queries must include a partition filter for partitioned tables"
    )
    @PluginProperty(group = "advanced")
    protected Property<Boolean> requirePartitionFilter;

    @Schema(
        title = "Encryption configuration",
        description = "Optional CMEK for the table"
    )
    @PluginProperty(group = "advanced")
    protected EncryptionConfiguration encryptionConfiguration;

    @Schema(
        title = "Expiration duration",
        description = "Duration from now until table expiration; if unset, table persists"
    )
    @PluginProperty(group = "execution")
    protected Property<Duration> expirationDuration;

    protected TableInfo.Builder build(TableInfo.Builder builder, RunContext runContext) throws Exception {
        if (this.tableDefinition != null) {
            builder.setDefinition(tableDefinition.to(runContext));
        }

        if (this.friendlyName != null) {
            builder.setFriendlyName(runContext.render(this.friendlyName).as(String.class).orElseThrow());
        }

        if (this.description != null) {
            builder.setDescription(runContext.render(this.description));
        }

        if (this.labels != null) {
            builder.setLabels(
                runContext.render(this.labels).asMap(String.class, String.class).entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            Rethrow.throwFunction(e -> runContext.render(e.getValue()))
                        )
                    )
            );
        }

        if (this.requirePartitionFilter != null) {
            builder.setRequirePartitionFilter(runContext.render(this.requirePartitionFilter).as(Boolean.class).orElseThrow());
        }

        if (this.encryptionConfiguration != null) {
            builder.setEncryptionConfiguration(this.encryptionConfiguration.to(runContext));
        }

        if (this.expirationDuration != null) {
            builder.setExpirationTime(Instant.now().plus(runContext.render(this.expirationDuration).as(Duration.class).orElseThrow()).toEpochMilli());
        }

        return builder;
    }
}
