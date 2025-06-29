package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a BigQuery table’s metadata."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_bq_table_metadata
                namespace: company.team

                tasks:
                  - id: table_metadata
                    type: io.kestra.plugin.gcp.bigquery.TableMetadata
                    projectId: my-project
                    dataset: my-dataset
                    table: my-table
                """
        )
    }
)
public class TableMetadata extends AbstractTable implements RunnableTask<AbstractTable.Output> {
    @Builder.Default
    @Schema(
        title = "Policy to apply if a table don't exists.",
        description = "If the policy is `SKIP`, the output will contain only null value, otherwise an error is raised."
    )
    private final Property<IfNotExists> ifNotExists = Property.ofValue(IfNotExists.ERROR);

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        TableId tableId = this.projectId != null  ?
            TableId.of(
                runContext.render(this.projectId).as(String.class).orElseThrow(),
                runContext.render(this.dataset).as(String.class).orElse(null),
                runContext.render(this.table).as(String.class).orElse(null)) :
            TableId.of(
                runContext.render(this.dataset).as(String.class).orElse(null),
                runContext.render(this.table).as(String.class).orElse(null));

        logger.debug("Getting table metadata '{}'", tableId);

        Table table = connection.getTable(tableId);

        var existValue = runContext.render(ifNotExists).as(IfNotExists.class).orElseThrow();
        if (table == null) {
            if (existValue == IfNotExists.ERROR) {
                throw new IllegalArgumentException("Unable to find table '" + tableId.getProject() + ":" + tableId.getDataset() + "." + tableId.getTable() + "'");
            } else if (existValue == IfNotExists.SKIP) {
                return Output.builder()
                    .build();
            }
        }

        return Output.of(Objects.requireNonNull(table));
    }

    public enum IfNotExists {
        ERROR,
        SKIP
    }
}
