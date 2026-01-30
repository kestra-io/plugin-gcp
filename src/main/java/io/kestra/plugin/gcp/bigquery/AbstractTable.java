package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.bigquery.models.EncryptionConfiguration;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.gcp.bigquery.models.TableDefinition;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractTable extends AbstractBigquery {
    @NotNull
    @Schema(
        title = "Dataset ID",
        description = "Target dataset name"
    )
    protected Property<String> dataset;

    @NotNull
    @Schema(
        title = "Table ID",
        description = "Target table name"
    )
    protected Property<String> table;

    protected TableId tableId(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.projectId != null  ?
            TableId.of(
                runContext.render(this.projectId).as(String.class).orElseThrow(),
                runContext.render(this.dataset).as(String.class).orElseThrow(),
                runContext.render(this.table).as(String.class).orElseThrow()) :
            TableId.of(
                runContext.render(this.dataset).as(String.class).orElseThrow(),
                runContext.render(this.table).as(String.class).orElseThrow());
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Project ID"
        )
        private final String projectId;

        @Schema(
            title = "Dataset ID"
        )
        private final String datasetId;

        @Schema(
            title = "Table name"
        )
        private final String table;

        @Schema(
            title = "Table etag"
        )
        private final String etag;

        @Schema(
            title = "Service-generated table id"
        )
        private final String generatedId;

        @Schema(
            title = "Self link URL",
            description = "API URL for get or update requests"
        )
        private final String selfLink;

        @Schema(
            title = "Friendly name"
        )
        private final String friendlyName;

        @Schema(
            title = "Description"
        )
        private final String description;

        @Schema(
            title = "Creation time"
        )
        private final Instant creationTime;

        @Schema(
            title = "Expiration time",
            description = "If absent, the table persists indefinitely; expired tables are deleted"
        )
        private final Instant expirationTime;

        @Schema(
            title = "Last modified time"
        )
        private final Instant lastModifiedTime;

        @Schema(
            title = "Size in bytes"
        )
        private final Long numBytes;

        @Schema(
            title = "Long-term storage bytes",
            description = "Bytes billed at long-term storage rates"
        )
        private final Long numLongTermBytes;

        @Schema(
            title = "Row count"
        )
        private final BigInteger numRows;

        @Schema(
            title = "Table definition"
        )
        private final TableDefinition.Output definition;

        @Schema(
            title = "Encryption configuration"
        )
        private final EncryptionConfiguration.Output encryptionConfiguration;

        @Schema(
            title = "Labels"
        )
        private final Map<String, String> labels;

        @Schema(
            title = "Require partition filter",
            description = "True when queries must specify a partition filter for partition elimination"
        )
        private final Boolean requirePartitionFilter;

        public static Output of(Table table) {
            return Output.builder()
                .projectId(table.getTableId().getProject())
                .datasetId(table.getTableId().getDataset())
                .table(table.getTableId().getTable())
                .etag(table.getEtag())
                .generatedId(table.getGeneratedId())
                .selfLink(table.getSelfLink())
                .friendlyName(table.getFriendlyName())
                .description(table.getDescription())
                .creationTime(table.getCreationTime() == null ? null : Instant.ofEpochMilli(table.getCreationTime()))
                .expirationTime(table.getExpirationTime() == null ? null : Instant.ofEpochMilli(table.getExpirationTime()))
                .lastModifiedTime(table.getLastModifiedTime() == null ? null : Instant.ofEpochMilli(table.getLastModifiedTime()))
                .numBytes(table.getNumBytes())
                .numLongTermBytes(table.getNumLongTermBytes())
                .numRows(table.getNumRows())
                .definition(TableDefinition.of(table.getDefinition()))
                .encryptionConfiguration(io.kestra.plugin.gcp.bigquery.models.EncryptionConfiguration.of(table.getEncryptionConfiguration()))
                .labels(table.getLabels())
                .requirePartitionFilter(table.getRequirePartitionFilter())
                .build();
        }
    }
}
