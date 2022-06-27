package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class TableDefinition {
    @Schema(title = "the table's type.")
    private final Type type;

    @Schema(title = "the table's schema.")
    private final io.kestra.plugin.gcp.bigquery.models.Schema schema;

    @Schema(title = "the table definition if the type is `TABLE`")
    @Builder.Default
    private final StandardTableDefinition standardTableDefinition = new StandardTableDefinition(null,null,null,null);

    @Schema(title = "the materialized view definition if the type is `MATERIALIZED_VIEW`")
    private final MaterializedViewDefinition materializedViewDefinition;

    @Schema(title = "the view definition if the type is `VIEW`")
    private final ViewDefinition viewDefinition;

    @Schema(title = "the external table definition if the type is `EXTERNAL`")
    private final ExternalTableDefinition externalTableDefinition;

    public static <T extends com.google.cloud.bigquery.TableDefinition> TableDefinition of(T tableDefinition) {
        TableDefinitionBuilder tableDefinitionBuilder = TableDefinition.builder()
            .type(Type.valueOf(tableDefinition.getType().toString()));

        if (tableDefinition.getSchema() != null) {
            tableDefinitionBuilder.schema(io.kestra.plugin.gcp.bigquery.models.Schema.of(tableDefinition.getSchema()));
        }

        if (tableDefinition instanceof com.google.cloud.bigquery.ViewDefinition) {
            var viewDefinition = ((com.google.cloud.bigquery.ViewDefinition) tableDefinition);

            tableDefinitionBuilder.viewDefinition(ViewDefinition.of(viewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.MaterializedViewDefinition) {
            var materializedViewDefinition = ((com.google.cloud.bigquery.MaterializedViewDefinition) tableDefinition);

            tableDefinitionBuilder.materializedViewDefinition(MaterializedViewDefinition.of(materializedViewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.ExternalTableDefinition) {
            var externalTableDefinition = ((com.google.cloud.bigquery.ExternalTableDefinition) tableDefinition);

            tableDefinitionBuilder.externalTableDefinition(ExternalTableDefinition.of(externalTableDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.StandardTableDefinition) {
            var standardTableDefinition = ((com.google.cloud.bigquery.StandardTableDefinition) tableDefinition);

            tableDefinitionBuilder.standardTableDefinition(StandardTableDefinition.of(standardTableDefinition));
        }

        return tableDefinitionBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public <T extends com.google.cloud.bigquery.TableDefinition> T to(RunContext runContext) throws Exception {
        switch (this.type) {
            case VIEW:
                return (T) this.viewDefinition.to(runContext);
            case TABLE:
                return (T) this.standardTableDefinition.to(runContext, this.schema);
            case EXTERNAL:
                return (T) this.externalTableDefinition.to(runContext, this.schema);
            case MATERIALIZED_VIEW:
                return (T) this.materializedViewDefinition.to(runContext);
            default:
                throw new Exception("Invalid type '" + this.type + "'");
        }
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
