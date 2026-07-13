package io.kestra.plugin.gcp.spanner;

import java.util.List;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    title = "Create a Spanner database",
    description = "Creates a Spanner database with optional DDL statements."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a Spanner database with a table",
            full = true,
            code = """
                id: spanner_create_database
                namespace: company.team

                tasks:
                  - id: create_db
                    type: io.kestra.plugin.gcp.spanner.CreateDatabase
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    extraDdl:
                      - |
                        CREATE TABLE users (
                          id INT64 NOT NULL,
                          name STRING(256),
                          age INT64
                        ) PRIMARY KEY (id)
                """
        )
    }
)
public class CreateDatabase extends AbstractSpanner implements RunnableTask<CreateDatabase.Output> {

    @Schema(title = "Optional list of DDL statements to run upon creation")
    @PluginProperty(group = "main")
    private Property<List<String>> extraDdl;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var dbId = this.databaseId(runContext);
        var rExtraDdl = this.extraDdl != null ? runContext.render(this.extraDdl).asList(String.class) : List.<String>of();

        try (var spanner = this.spannerClient(runContext)) {
            var adminClient = spanner.getDatabaseAdminClient();
            var op = adminClient.createDatabase(
                dbId.getInstanceId().getInstance(),
                dbId.getDatabase(),
                rExtraDdl
            );
            op.get();
        }

        return Output.builder()
            .database(dbId.getDatabase())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The database name")
        private final String database;
    }
}
