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
    title = "Execute a DML or DDL statement.",
    description = "Executes an INSERT, UPDATE, DELETE statement, or schema change DDL statement."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute DML to update records.",
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rSql = runContext.render(this.sql).as(String.class).orElseThrow();
        String cleanSql = rSql.trim().toLowerCase();
        boolean isDdl = cleanSql.startsWith("create") || cleanSql.startsWith("alter") || cleanSql.startsWith("drop");

        Long affectedRows = 0L;

        try (Spanner spanner = this.spannerClient(runContext)) {
            if (isDdl) {
                DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
                DatabaseId dbId = this.databaseId(runContext);
                OperationFuture<Void, UpdateDatabaseDdlMetadata> op = adminClient.updateDatabaseDdl(
                    dbId.getInstanceId().getInstance(),
                    dbId.getDatabase(),
                    List.of(rSql),
                    null
                );
                op.get(); // wait for the operation to complete
            } else {
                DatabaseClient dbClient = spanner.getDatabaseClient(this.databaseId(runContext));
                affectedRows = dbClient.readWriteTransaction().run(transaction -> {
                    Statement.Builder stmtBuilder = Statement.newBuilder(rSql);
                    if (this.parameters != null) {
                        Map<String, Object> rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
                        for (Map.Entry<String, Object> entry : rParams.entrySet()) {
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
