package io.kestra.plugin.gcp.billing;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.bigquery.Query;
import io.kestra.plugin.gcp.shared.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query GCP BigQuery Billing Export",
    description = "Executes a parameterized cost aggregation query over standard or detailed GCP BigQuery billing export tables."
)
@Plugin(
    examples = {
        @Example(
            title = "Aggregate monthly costs by service description",
            full = true,
            code = """
                id: gcp_monthly_cost
                namespace: company.finops

                tasks:
                  - id: cost_by_service
                    type: io.kestra.plugin.gcp.billing.BillingExportQuery
                    projectId: "{{ secret('GCP_PROJECT') }}"
                    serviceAccount: "{{ secret('GCP_SA') }}"
                    exportTable: "billing.gcp_billing_export_v1_0123AB_4567CD_89EF01"
                    dateFrom: "{{ now() | dateAdd(-1, 'MONTHS') | date('yyyy-MM-dd') }}"
                    groupBy:
                      - "service.description"
                    fetchType: STORE
                """
        )
    }
)
public class BillingExportQuery extends Task implements GcpInterface, RunnableTask<BillingExportQuery.Output> {

    @Schema(title = "The GCP project ID")
    @PluginProperty(group = "connection")
    protected Property<String> projectId;

    @Schema(title = "The GCP service account key")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> serviceAccount;

    @Schema(title = "The GCP service account to impersonate")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> impersonatedServiceAccount;

    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "connection")
    protected Property<List<String>> scopes;

    @NotNull
    @Schema(
        title = "BigQuery Billing Export table",
        description = "Fully-qualified table name (e.g. `project.dataset.gcp_billing_export_v1_XXXXXX` or `dataset.gcp_billing_export_resource_v1_XXXXXX`)"
    )
    @PluginProperty(group = "source")
    private Property<String> exportTable;

    @NotNull
    @Schema(
        title = "Query start date or timestamp",
        description = "Inclusive start timestamp or date (e.g. `2026-09-01` or `2026-09-01T00:00:00Z`)"
    )
    @PluginProperty(group = "source")
    private Property<String> dateFrom;

    @Schema(
        title = "Query end date or timestamp",
        description = "Exclusive end timestamp or date (e.g. `2026-10-01` or `2026-10-01T00:00:00Z`)"
    )
    @PluginProperty(group = "source")
    private Property<String> dateTo;

    @Schema(
        title = "Fields to group by",
        description = "List of fields or expressions to group costs by (e.g. `['service.description']`, `['project.id', 'sku.description']`)"
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> groupBy;

    @Schema(
        title = "Additional SQL filter expression",
        description = "Custom WHERE clause conditions to apply (e.g. `project.id = 'my-project-id'`)"
    )
    @PluginProperty(group = "processing")
    private Property<String> filter;

    @Builder.Default
    @Schema(
        title = "Include credits calculation",
        description = "Whether to calculate net cost including promotions, discounts, and credits (default: true)"
    )
    @PluginProperty(group = "processing")
    private Property<Boolean> includeCredits = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Result handling mode",
        description = "`FETCH` by default. Use `STORE` to save large result sets to Kestra internal storage."
    )
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "Destination BigQuery table",
        description = "Optional destination table to write query results to"
    )
    @PluginProperty(group = "destination")
    private Property<String> destinationTable;

    public static String buildSql(
        String exportTable,
        String dateFrom,
        String dateTo,
        List<String> groupBy,
        String filter,
        boolean includeCredits
    ) {
        var cleanTable = exportTable.trim();
        if (!cleanTable.startsWith("`")) {
            cleanTable = "`" + cleanTable + "`";
        }

        var selectParts = new ArrayList<String>();
        var groupByParts = new ArrayList<String>();

        if (groupBy != null && !groupBy.isEmpty()) {
            for (var col : groupBy) {
                var trimmed = col.trim();
                selectParts.add(trimmed);
                groupByParts.add(trimmed);
            }
        }

        selectParts.add("currency");
        groupByParts.add("currency");

        selectParts.add("SUM(cost) AS total_cost");
        if (includeCredits) {
            selectParts.add("SUM(cost) + IFNULL(SUM((SELECT SUM(c.amount) FROM UNNEST(credits) c)), 0) AS net_cost");
        }

        var whereParts = new ArrayList<String>();
        whereParts.add("usage_start_time >= TIMESTAMP('" + dateFrom.trim() + "')");
        if (dateTo != null && !dateTo.trim().isEmpty()) {
            whereParts.add("usage_start_time < TIMESTAMP('" + dateTo.trim() + "')");
        }
        if (filter != null && !filter.trim().isEmpty()) {
            whereParts.add("(" + filter.trim() + ")");
        }

        var sql = new StringBuilder();
        sql.append("SELECT\n  ")
            .append(String.join(",\n  ", selectParts))
            .append("\nFROM ")
            .append(cleanTable)
            .append("\nWHERE ")
            .append(String.join("\n  AND ", whereParts))
            .append("\nGROUP BY ")
            .append(String.join(", ", groupByParts))
            .append("\nORDER BY ")
            .append(includeCredits ? "net_cost" : "total_cost")
            .append(" DESC");

        return sql.toString();
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var renderedTable = runContext.render(this.exportTable).as(String.class).orElseThrow();
        var renderedDateFrom = runContext.render(this.dateFrom).as(String.class).orElseThrow();
        var renderedDateTo = runContext.render(this.dateTo).as(String.class).orElse(null);
        var renderedGroupBy = runContext.render(this.groupBy).asList(String.class);
        var renderedFilter = runContext.render(this.filter).as(String.class).orElse(null);
        var renderedIncludeCredits = runContext.render(this.includeCredits).as(Boolean.class).orElse(true);

        var generatedSql = buildSql(
            renderedTable,
            renderedDateFrom,
            renderedDateTo,
            renderedGroupBy,
            renderedFilter,
            renderedIncludeCredits
        );

        logger.debug("Executing generated BigQuery billing export query:\n{}", generatedSql);

        var internalQuery = Query.builder()
            .id(this.getId() + "_query")
            .type(Query.class.getName())
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .impersonatedServiceAccount(this.impersonatedServiceAccount)
            .scopes(this.scopes)
            .sql(Property.ofValue(generatedSql))
            .fetchType(this.fetchType)
            .destinationTable(this.destinationTable)
            .build();

        var queryOutput = internalQuery.run(runContext);

        return Output.builder()
            .rows(queryOutput.getRows())
            .row(queryOutput.getRow())
            .uri(queryOutput.getUri())
            .size(queryOutput.getSize())
            .jobId(queryOutput.getJobId())
            .sql(generatedSql)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of fetched result rows when fetchType is FETCH")
        private final List<Map<String, Object>> rows;

        @Schema(title = "Single fetched result row when fetchType is FETCH_ONE")
        private final Map<String, Object> row;

        @Schema(title = "Kestra internal storage URI when fetchType is STORE")
        private final URI uri;

        @Schema(title = "The number of rows returned by the query")
        private final Long size;

        @Schema(title = "The BigQuery job ID")
        private final String jobId;

        @Schema(title = "The generated SQL query executed against BigQuery")
        private final String sql;
    }
}
