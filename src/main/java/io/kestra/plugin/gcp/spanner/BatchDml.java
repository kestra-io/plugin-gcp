package io.kestra.plugin.gcp.spanner;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.spanner.*;

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
    title = "Execute multiple DML statements atomically.",
    description = "Executes multiple INSERT, UPDATE, or DELETE statements in a single read-write transaction."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute batch DML statements.",
            full = true,
            code = """
                id: spanner_batch_dml
                namespace: company.team

                tasks:
                  - id: batch_update
                    type: io.kestra.plugin.gcp.spanner.BatchDml
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    statements:
                      - "UPDATE users SET active = true WHERE signup_date > '2023-01-01'"
                      - "DELETE FROM temp_sessions WHERE expires_at < CURRENT_TIMESTAMP()"
                """
        )
    }
)
public class BatchDml extends AbstractSpanner implements RunnableTask<BatchDml.Output> {

    @NotNull
    @Schema(
        title = "The list of SQL DML statements to execute",
        description = "Statements are executed sequentially without parameter bindings. Parameterized batch DML is not supported."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> statements;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rStatements = runContext.render(this.statements).asList(String.class);
        var affectedRows = new ArrayList<Long>();
        long totalAffectedRows = 0;

        try (var spanner = this.spannerClient(runContext)) {
            var dbClient = spanner.getDatabaseClient(this.databaseId(runContext));
            var results = dbClient.readWriteTransaction().run(transaction -> {
                var statementList = rStatements.stream()
                    .map(Statement::of)
                    .toList();
                return transaction.batchUpdate(statementList);
            });

            for (var result : results) {
                affectedRows.add(result);
                totalAffectedRows += result;
            }
        }

        return Output.builder()
            .affectedRows(affectedRows)
            .totalAffectedRows(totalAffectedRows)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of counts of affected rows for each statement")
        private final List<Long> affectedRows;

        @Schema(title = "Total number of rows affected by all statements combined")
        private final Long totalAffectedRows;
    }
}
