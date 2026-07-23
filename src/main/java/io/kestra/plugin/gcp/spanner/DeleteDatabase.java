package io.kestra.plugin.gcp.spanner;

import com.google.cloud.spanner.*;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a Spanner database",
    description = "Permanently deletes a Spanner database and all of its schema and data."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a Spanner database",
            full = true,
            code = """
                id: spanner_delete_database
                namespace: company.team

                tasks:
                  - id: delete_db
                    type: io.kestra.plugin.gcp.spanner.DeleteDatabase
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                """
        )
    }
)
public class DeleteDatabase extends AbstractSpanner implements RunnableTask<DeleteDatabase.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var dbId = this.databaseId(runContext);

        try (var spanner = this.spannerClient(runContext)) {
            var adminClient = spanner.getDatabaseAdminClient();
            adminClient.dropDatabase(dbId.getInstanceId().getInstance(), dbId.getDatabase());
        }

        return Output.builder()
            .database(dbId.getDatabase())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The database name that was deleted")
        private final String database;
    }
}
