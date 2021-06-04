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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a table"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "projectId: my-project",
                "dataset: my-dataset",
                "table: my-table",
                "tableDefinition:",
                "  type: TABLE",
                "  schema:",
                "    fields:",
                "    - name: id",
                "      type: INT64",
                "    - name: name",
                "      type: STRING",
                "  standardTableDefinition:",
                "    clustering:",
                "    - id",
                "    - name",
                "friendlyName: new_table"
            }
        )
    }
)
public class CreateTable extends AbstractTableCreateUpdate implements RunnableTask<AbstractTable.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.tableId(runContext);

        TableInfo.Builder builder = TableInfo.newBuilder(tableId, this.tableDefinition.to(runContext));
        builder = this.build(builder, runContext);

        TableInfo tableInfo = builder.build();

        logger.debug("Creating table '{}'", tableId);

        Table table = connection.create(tableInfo);

        return Output.of(Objects.requireNonNull(table));
    }
}
