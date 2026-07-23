package io.kestra.plugin.gcp.spanner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import com.google.cloud.spanner.*;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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
    title = "Execute a Spanner SQL query",
    description = "Queries database tables and exposes results downstream or dumps to internal storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Query a Spanner table and store results",
            full = true,
            code = """
                id: spanner_query
                namespace: company.team

                inputs:
                  - id: department
                    type: STRING
                    defaults: engineering

                tasks:
                  - id: query
                    type: io.kestra.plugin.gcp.spanner.Query
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    sql: "SELECT * FROM users WHERE department = @department"
                    parameters:
                      department: "{{ inputs.department }}"
                    fetchType: STORE
                """
        )
    }
)
public class Query extends AbstractSpanner implements RunnableTask<Query.Output> {

    @NotNull
    @Schema(title = "The SQL query to execute")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(title = "Query parameters")
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> parameters;

    @Builder.Default
    @Schema(title = "Result handling mode")
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rSql = runContext.render(this.sql).as(String.class).orElseThrow();
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.NONE);

        var stmtBuilder = Statement.newBuilder(rSql);
        if (this.parameters != null) {
            var rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
            for (var entry : rParams.entrySet()) {
                bindParameter(stmtBuilder, entry.getKey(), entry.getValue());
            }
        }

        var statement = stmtBuilder.build();
        var outputBuilder = Output.builder();

        try (var spanner = this.spannerClient(runContext)) {
            var dbClient = spanner.getDatabaseClient(this.databaseId(runContext));
            try (var resultSet = dbClient.singleUse().executeQuery(statement)) {
                if (FetchType.STORE.equals(rFetchType)) {
                    var stored = this.storeResult(resultSet, runContext);
                    outputBuilder.uri(stored.getKey()).size(stored.getValue());
                } else {
                    var fetch = new ArrayList<Map<String, Object>>();
                    while (resultSet.next()) {
                        fetch.add(rowToMap(resultSet.getCurrentRowAsStruct()));
                    }
                    outputBuilder.size((long) fetch.size());

                    if (FetchType.FETCH.equals(rFetchType)) {
                        outputBuilder.rows(fetch);
                    } else if (FetchType.FETCH_ONE.equals(rFetchType)) {
                        outputBuilder.row(fetch.isEmpty() ? new LinkedHashMap<>() : fetch.get(0));
                    }
                }
            }
        }

        return outputBuilder.build();
    }

    private Map.Entry<URI, Long> storeResult(ResultSet resultSet, RunContext runContext) throws IOException {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        var lineCount = 0L;
        try (
            var output = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)
        ) {
            while (resultSet.next()) {
                FileSerde.write(output, rowToMap(resultSet.getCurrentRowAsStruct()));
                lineCount++;
            }
        }
        return new AbstractMap.SimpleEntry<>(
            runContext.storage().putFile(tempFile),
            lineCount
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List containing the fetched data")
        private List<Map<String, Object>> rows;

        @Schema(title = "Map containing the first row of fetched data")
        private Map<String, Object> row;

        @Schema(title = "The size of the rows fetch")
        private Long size;

        @Schema(title = "The URI of stored result")
        private URI uri;
    }
}
