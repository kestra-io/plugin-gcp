package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Getter
@Builder
@Jacksonized
public class TableDefinition {
    @Schema(title = "The table's type.")
    private final Property<Type> type;

    @Schema(title = "The table's schema.")
    private final io.kestra.plugin.gcp.bigquery.models.Schema schema;

    @Schema(title = "The table definition if the type is `TABLE`.")
    private final StandardTableDefinition standardTableDefinition;

    @Schema(title = "The materialized view definition if the type is `MATERIALIZED_VIEW`.")
    private final MaterializedViewDefinition materializedViewDefinition;

    @Schema(title = "The view definition if the type is `VIEW`.")
    private final ViewDefinition viewDefinition;

    @Schema(title = "The external table definition if the type is `EXTERNAL`.")
    private final ExternalTableDefinition externalTableDefinition;

    public static <T extends com.google.cloud.bigquery.TableDefinition> TableDefinition.Output of(T tableDefinition) {
        TableDefinition.Output.OutputBuilder outputBuilder = Output.builder()
            .type(Type.valueOf(tableDefinition.getType().toString()));

        if (tableDefinition.getSchema() != null) {
            outputBuilder.schema(io.kestra.plugin.gcp.bigquery.models.Schema.of(tableDefinition.getSchema()));
        }

        if (tableDefinition instanceof com.google.cloud.bigquery.ViewDefinition viewDefinition) {
            outputBuilder.viewDefinition(ViewDefinition.of(viewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.MaterializedViewDefinition materializedViewDefinition) {
            outputBuilder.materializedViewDefinition(MaterializedViewDefinition.of(materializedViewDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.ExternalTableDefinition externalTableDefinition) {
            outputBuilder.externalTableDefinition(ExternalTableDefinition.of(externalTableDefinition));
        } else if (tableDefinition instanceof com.google.cloud.bigquery.StandardTableDefinition standardTableDefinition) {
            outputBuilder.standardTableDefinition(StandardTableDefinition.of(standardTableDefinition));
        }

        return outputBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public <T extends com.google.cloud.bigquery.TableDefinition> T to(RunContext runContext) throws Exception {
        switch (runContext.render(this.type).as(Type.class).orElse(null)) {
            case VIEW:
                return (T) this.viewDefinition.to(runContext);
            case TABLE:
                return (T) (this.standardTableDefinition == null ? StandardTableDefinition.builder().build() : this.standardTableDefinition)
                    .to(runContext, this.schema);
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

    @Builder
    @Getter
    public static class Output {
        private final Type type;
        private final io.kestra.plugin.gcp.bigquery.models.Schema.Output schema;
        private final ViewDefinition.Output viewDefinition;
        private final ExternalTableDefinition.Output externalTableDefinition;
        private final StandardTableDefinition.Output standardTableDefinition;
        private final MaterializedViewDefinition.Output materializedViewDefinition;
    }
}
