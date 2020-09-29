package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.*;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.task.gcp.bigquery.models.TableDefinition;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class AbstractTable extends AbstractBigquery implements RunnableTask<AbstractTable.Output> {
    @NotNull
    @InputProperty(
        description = "The dataset's user-defined id",
        dynamic = true
    )
    protected String dataset;

    @NotNull
    @InputProperty(
        description = "The table user-defined id",
        dynamic = true
    )
    protected String table;

    @Getter
    @Builder
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The project's id"
        )
        private final String projectId;

        @OutputProperty(
            description = "The dataset's id"
        )
        private final String datasetId;

        @OutputProperty(
            description = "The table name"
        )
        private final String table;

        @OutputProperty(
            description = "The hash of the table resource"
        )
        private final String etag;

        @OutputProperty(
            description = "The service-generated id for the table"
        )
        private final String generatedId;

        @OutputProperty(
            description = "The URL that can be used to access the resource again. The returned URL can be used for get or update requests."
        )
        private final String selfLink;

        @OutputProperty(
            description = "The user-friendly name for the table"
        )
        private final String friendlyName;

        @OutputProperty(
            description = "The user-friendly description for the table"
        )
        private final String description;

        @OutputProperty(
            description = "The time when this table was created"
        )
        private final Instant creationTime;

        @OutputProperty(
            description = "Returns the time when this table expires",
            body = "If not present, the table will persist indefinitely. Expired tables will be deleted and their storage reclaimed."
        )
        private final Instant expirationTime;

        @OutputProperty(
            description = "The time when this table was last modified"
        )
        private final Instant lastModifiedTime;

        @OutputProperty(
            description = "The size of this table in bytes"
        )
        private final Long numBytes;

        @OutputProperty(
            description = "The number of bytes considered \"long-term storage\" for reduced billing purposes."
        )
        private final Long numLongTermBytes;

        @OutputProperty(
            description = "The number of rows of data in this table"
        )
        private final BigInteger numRows;

        @OutputProperty(
            description = "The table definition"
        )
        private final org.kestra.task.gcp.bigquery.models.TableDefinition definition;

        @OutputProperty(
            description = "The encryption configuration"
        )
        private final org.kestra.task.gcp.bigquery.models.EncryptionConfiguration encryptionConfiguration;

        @OutputProperty(
            description = "Return a map for labels applied to the table"
        )
        private final Map<String, String> labels;

        @OutputProperty(
            description = "Return true if a partition filter (that can be used for partition elimination) " +
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
                .creationTime(Instant.ofEpochMilli(table.getCreationTime()))
                .expirationTime(Instant.ofEpochMilli(table.getExpirationTime()))
                .lastModifiedTime(Instant.ofEpochMilli(table.getLastModifiedTime()))
                .numBytes(table.getNumBytes())
                .numLongTermBytes(table.getNumLongTermBytes())
                .numRows(table.getNumRows())
                .definition(TableDefinition.of(table.getDefinition()))
                .encryptionConfiguration(org.kestra.task.gcp.bigquery.models.EncryptionConfiguration.of(table.getEncryptionConfiguration()))
                .labels(table.getLabels())
                .requirePartitionFilter(table.getRequirePartitionFilter())
                .build();
        }
    }


}
