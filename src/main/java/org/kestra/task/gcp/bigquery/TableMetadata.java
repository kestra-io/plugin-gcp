package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Get table metadata"
)
public class TableMetadata extends AbstractTable {
    @Builder.Default
    @InputProperty(
        description = "Policy to apply if a table don't exists."
    )
    private final IfExists ifExists = IfExists.ERROR;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.projectId != null  ?
            TableId.of(runContext.render(this.projectId), runContext.render(this.dataset), runContext.render(this.table)) :
            TableId.of(runContext.render(this.dataset), runContext.render(this.table));

        logger.debug("Getting table metadata '{}'", tableId);

        Table table = connection.getTable(tableId);

        if (ifExists == IfExists.ERROR && table == null) {
            throw new IllegalArgumentException("Unable to find table '" + tableId.getProject() + ":" + tableId.getDataset() + "." + tableId.getTable() + "'");
        } else if (ifExists == IfExists.SKIP) {
            return Output.builder()
                .build();
        }

        return Output.of(table);
    }

    public enum IfExists {
        ERROR,
        SKIP
    }
}
