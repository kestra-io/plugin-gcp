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
    title = "Execute a Spanner SQL query.",
    description = "Queries database tables and exposes results downstream or dumps to internal storage."
)
@Plugin(
    examples = {
        @Example(
            title = "Query a Spanner table and store results.",
            full = true,
            code = """
                id: spanner_query
                namespace: company.team

                inputs:
                  - id: min_age
                    type: INT
                    defaults: 18

                tasks:
                  - id: query
                    type: io.kestra.plugin.gcp.spanner.Query
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    instanceId: my-instance
                    databaseId: my-database
                    sql: "SELECT * FROM users WHERE age >= @minAge"
                    parameters:
                      minAge: "{{ inputs.min_age }}"
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
        String rSql = runContext.render(this.sql).as(String.class).orElseThrow();
        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.NONE);

        Statement.Builder stmtBuilder = Statement.newBuilder(rSql);
        if (this.parameters != null) {
            Map<String, Object> rParams = runContext.render(this.parameters).asMap(String.class, Object.class);
            for (Map.Entry<String, Object> entry : rParams.entrySet()) {
                bindParameter(stmtBuilder, entry.getKey(), entry.getValue());
            }
        }

        Statement statement = stmtBuilder.build();
        Output.OutputBuilder outputBuilder = Output.builder();

        try (Spanner spanner = this.spannerClient(runContext)) {
            DatabaseClient dbClient = spanner.getDatabaseClient(this.databaseId(runContext));
            try (ResultSet resultSet = dbClient.singleUse().executeQuery(statement)) {
                if (FetchType.STORE.equals(rFetchType)) {
                    Map.Entry<URI, Long> stored = this.storeResult(resultSet, runContext);
                    outputBuilder.uri(stored.getKey()).size(stored.getValue());
                } else {
                    List<Map<String, Object>> fetch = new ArrayList<>();
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
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        long lineCount = 0;
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
