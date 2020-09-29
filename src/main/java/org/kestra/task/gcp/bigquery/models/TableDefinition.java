package org.kestra.task.gcp.bigquery.models;

import com.google.cloud.bigquery.Schema;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

@Getter
@Builder
public class TableDefinition {
    @OutputProperty(description = "Returns the table's type.")
    private final Type type;

    @OutputProperty(description = "Returns the table's schema.")
    private final Schema schema;

    @OutputProperty(description = "Returns the table definition if the type is `TABLE`")
    private final StandardTableDefinition standardTableDefinition;

    @OutputProperty(description = "Returns the materialized view definition if the type is `MATERIALIZED_VIEW`")
    private final MaterializedViewDefinition materializedViewDefinition;

    @OutputProperty(description = "Returns the view definition if the type is `VIEW`")
    private final ViewDefinition viewDefinition;

    @OutputProperty(description = "Returns the external table definition if the type is `EXTERNAL`")
    private final ExternalTableDefinition externalTableDefinition;

    public static <T extends com.google.cloud.bigquery.TableDefinition> TableDefinition of(T tableDefinition) {
        TableDefinitionBuilder tableDefinitionBuilder = TableDefinition.builder()
            .type(Type.valueOf(tableDefinition.getType().toString()))
            .schema(tableDefinition.getSchema());

        if (tableDefinition instanceof com.google.cloud.bigquery.ViewDefinition) {
            var viewDefinition = ((com.google.cloud.bigquery.ViewDefinition) tableDefinition);
            tableDefinitionBuilder
                .viewDefinition(ViewDefinition.of(viewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.MaterializedViewDefinition) {
            var materializedViewDefinition = ((com.google.cloud.bigquery.MaterializedViewDefinition) tableDefinition);

            tableDefinitionBuilder.materializedViewDefinition(MaterializedViewDefinition.of(materializedViewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.ExternalTableDefinition) {
            var externalTableDefinition = ((com.google.cloud.bigquery.ExternalTableDefinition) tableDefinition);

            tableDefinitionBuilder.externalTableDefinition(ExternalTableDefinition.of(externalTableDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.StandardTableDefinition) {
            var standardTableDefinition = ((com.google.cloud.bigquery.StandardTableDefinition) tableDefinition);
            tableDefinitionBuilder
                .standardTableDefinition(StandardTableDefinition.of(standardTableDefinition));
        }

        return tableDefinitionBuilder.build();
    }

    @SuppressWarnings("unused")
    public enum Type {
        TABLE,
        VIEW,
        MATERIALIZED_VIEW,
        EXTERNAL,
        MODEL
    }
}
