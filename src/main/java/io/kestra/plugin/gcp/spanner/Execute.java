package io.kestra.plugin.gcp.spanner;

import java.util.List;
import java.util.Map;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a DML or DDL statement",
    description = "Executes an INSERT, UPDATE, DELETE statement, or schema change DDL statement."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute DML to update records",
            full = true,
            code = """
                id: spanner_execute_dml
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.gcp.spanner.Execute
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    sql: "UPDATE orders SET status = 'processed' WHERE processed_at IS NULL"
                """
        )
    }
)
public class Execute extends AbstractSpanner implements RunnableTask<Execute.Output> {

    @NotNull
    @Schema(title = "The SQL statement to execute")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(title = "Query parameters (only applicable to DML)")
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> parameters;

    @Builder.Default
    @Schema(title = "Whether the statement is a DDL statement")
    @PluginProperty(group = "main")
    private Property<Boolean> isDdl = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rSql = runContext.render(this.sql).as(String.class).orElseThrow();
        var rIsDdl = runContext.render(this.isDdl).as(Boolean.class).orElse(false);

        var affectedRows = 0L;

        try (var spanner = this.spannerClient(runContext)) {
            if (rIsDdl) {
                var adminClient = spanner.getDatabaseAdminClient();
                var dbId = this.databaseId(runContext);
                var op = adminClient.updateDatabaseDdl(
                    dbId.getInstanceId().getInstance(),
                    dbId.getDatabase(),
                    List.of(rSql),
                    null
                );
                op.get();
            } else {
                var dbClient = spanner.getDatabaseClient(this.databaseId(runContext));
                affectedRows = dbClient.readWriteTransaction().run(transaction -> {
                    var stmtBuilder = Statement.newBuilder(rSql);
                    if (this.parameters != null) {
                        var rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
                        for (var entry : rParams.entrySet()) {
                            bindParameter(stmtBuilder, entry.getKey(), entry.getValue());
                        }
                    }
                    return transaction.executeUpdate(stmtBuilder.build());
                });
            }
        }

        return Output.builder()
            .affectedRows(affectedRows)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The number of rows affected by the statement (DML only)")
        private final Long affectedRows;
    }
}
