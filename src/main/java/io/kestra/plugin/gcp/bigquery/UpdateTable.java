package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update table metadata"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_bq_update_table
                namespace: company.name

                tasks:
                  - id: update_table
                    type: io.kestra.plugin.gcp.bigquery.UpdateTable
                    projectId: my-project
                    dataset: my-dataset
                    table: my-table
                    expirationDuration: PT48H
                """
        )
    }
)
public class UpdateTable extends AbstractTableCreateUpdate implements RunnableTask<AbstractTable.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        Table table = connection.getTable(tableId);

        TableInfo.Builder builder = TableInfo.newBuilder(tableId, table.getDefinition());
        builder = this.build(builder, runContext);

        TableInfo tableInfo = builder.build();

        logger.debug("Updating table metadata '{}'", tableId);

        table = connection.update(tableInfo);

        return Output.of(Objects.requireNonNull(table));
    }
}
