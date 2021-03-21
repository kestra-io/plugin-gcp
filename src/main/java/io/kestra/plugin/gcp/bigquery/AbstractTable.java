package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.Table;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.gcp.bigquery.models.TableDefinition;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractTable extends AbstractBigquery {
    @NotNull
    @Schema(
        title = "The dataset's user-defined id"
    )
    @PluginProperty(dynamic = true)
    protected String dataset;

    @NotNull
    @Schema(
        title = "The table user-defined id"
    )
    @PluginProperty(dynamic = true)
    protected String table;

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The project's id"
        )
        private final String projectId;

        @Schema(
            title = "The dataset's id"
        )
        private final String datasetId;

        @Schema(
            title = "The table name"
        )
        private final String table;

        @Schema(
            title = "The hash of the table resource"
        )
        private final String etag;

        @Schema(
            title = "The service-generated id for the table"
        )
        private final String generatedId;

        @Schema(
            title = "The URL that can be used to access the resource again. The returned URL can be used for get or update requests."
        )
        private final String selfLink;

        @Schema(
            title = "The user-friendly name for the table"
        )
        private final String friendlyName;

        @Schema(
            title = "The user-friendly description for the table"
        )
        private final String description;

        @Schema(
            title = "The time when this table was created"
        )
        private final Instant creationTime;

        @Schema(
            title = "Returns the time when this table expires",
            description = "If not present, the table will persist indefinitely. Expired tables will be deleted and their storage reclaimed."
        )
        private final Instant expirationTime;

        @Schema(
            title = "The time when this table was last modified"
        )
        private final Instant lastModifiedTime;

        @Schema(
            title = "The size of this table in bytes"
        )
        private final Long numBytes;

        @Schema(
            title = "The number of bytes considered \"long-term storage\" for reduced billing purposes."
        )
        private final Long numLongTermBytes;

        @Schema(
            title = "The number of rows of data in this table"
        )
        private final BigInteger numRows;

        @Schema(
            title = "The table definition"
        )
        private final io.kestra.plugin.gcp.bigquery.models.TableDefinition definition;

        @Schema(
            title = "The encryption configuration"
        )
        private final io.kestra.plugin.gcp.bigquery.models.EncryptionConfiguration encryptionConfiguration;

        @Schema(
            title = "Return a map for labels applied to the table"
        )
        private final Map<String, String> labels;

        @Schema(
            title = "Return true if a partition filter (that can be used for partition elimination) " +
                "is required for queries over this table."
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
