package org.kestra.task.gcp.bigquery.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import org.kestra.core.serializers.JacksonMapper;

import java.util.Map;

@Getter
@Builder
public class TableDefinition {
    @Schema(title = "Returns the table's type.")
    private final Type type;

    @Schema(title = "Returns the table's schema.")
    private final Map<String, Object> schema;

    @Schema(title = "Returns the table definition if the type is `TABLE`")
    private final StandardTableDefinition standardTableDefinition;

    @Schema(title = "Returns the materialized view definition if the type is `MATERIALIZED_VIEW`")
    private final MaterializedViewDefinition materializedViewDefinition;

    @Schema(title = "Returns the view definition if the type is `VIEW`")
    private final ViewDefinition viewDefinition;

    @Schema(title = "Returns the external table definition if the type is `EXTERNAL`")
    private final ExternalTableDefinition externalTableDefinition;

    public static <T extends com.google.cloud.bigquery.TableDefinition> TableDefinition of(T tableDefinition) {
        TableDefinitionBuilder tableDefinitionBuilder = TableDefinition.builder()
            .type(Type.valueOf(tableDefinition.getType().toString()))
            .schema(JacksonMapper.toMap(tableDefinition.getSchema()));

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
