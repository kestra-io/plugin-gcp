package io.kestra.plugin.gcp.billing;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.bigquery.Query;

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
    description = """
        Executes a parameterized cost aggregation query over standard or detailed GCP BigQuery billing export tables."""
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
                    serviceAccount: "{{ secret('GCP_SA') }}"
                    exportTable: "my-project.billing.gcp_billing_export_v1_0123AB_4567CD_89EF01"
                    dateFrom: "{{ now() | dateAdd(-1, 'MONTHS') | date('yyyy-MM-dd') }}"
                    groupBy:
                      - "service.description"
                    fetchType: STORE
                """
        )
    }
)
public class BillingExportQuery extends AbstractBilling implements RunnableTask<BillingExportQuery.Output> {

    private static final Pattern TABLE_PATTERN = Pattern.compile("^[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+){1,2}$");

    @NotNull
    @Schema(
        title = "BigQuery Billing Export table",
        description = """
            Fully-qualified table name (e.g. `my-project.dataset.gcp_billing_export_v1_XXXXXX` or \
            `dataset.gcp_billing_export_v1_XXXXXX`)."""
    )
    @PluginProperty(group = "source")
    private Property<String> exportTable;

    @NotNull
    @Schema(
        title = "Query start date or timestamp",
        description = """
            Inclusive start timestamp or date (e.g. `2026-09-01` or `2026-09-01T00:00:00Z`). \
            Will be parsed and validated as an ISO-8601 date or timestamp."""
    )
    @PluginProperty(group = "source")
    private Property<String> dateFrom;

    @Schema(
        title = "Query end date or timestamp",
        description = """
            Exclusive end timestamp or date (e.g. `2026-10-01` or `2026-10-01T00:00:00Z`). \
            Will be parsed and validated as an ISO-8601 date or timestamp."""
    )
    @PluginProperty(group = "source")
    private Property<String> dateTo;

    @Schema(
        title = "Fields to group by",
        description = """
            (Raw SQL fragments or column expressions) List of columns or expressions to group costs by \
            (e.g. `['service.description']`, `['project.id', 'sku.description']`). Dot-notated struct fields \
            are automatically aliased (e.g. `service.description AS service_description`) to prevent duplicate \
            column names in BigQuery."""
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> groupBy;

    @Schema(
        title = "Additional SQL filter expression",
        description = """
            (Raw SQL fragment) Optional custom WHERE clause conditions to apply \
            (e.g. `project.id = 'my-project-id'`)."""
    )
    @PluginProperty(group = "processing")
    private Property<String> filter;

    @Builder.Default
    @Schema(
        title = "Include credits calculation",
        description = """
            Whether to calculate net cost including promotions, discounts, and credits (default: true)."""
    )
    @PluginProperty(group = "processing")
    private Property<Boolean> includeCredits = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Result handling mode",
        description = """
            `FETCH` by default. Use `STORE` to save large result sets to Kestra internal storage."""
    )
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "Destination BigQuery table",
        description = """
            Optional destination table to write query results to."""
    )
    @PluginProperty(group = "destination")
    private Property<String> destinationTable;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Query> queryTask = new AtomicReference<>();

    public static String parseCanonicalTimestamp(String input, String propertyName) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("Property '" + propertyName + "' must not be empty");
        }
        var trimmed = input.trim();
        try {
            return LocalDate.parse(trimmed).atStartOfDay(ZoneOffset.UTC).toInstant().toString();
        } catch (DateTimeParseException e1) {
            try {
                return Instant.parse(trimmed).toString();
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException(
                    "Invalid date format for property '" + propertyName + "': '" + input +
                    "'. Expected ISO-8601 Date (YYYY-MM-DD) or Timestamp (YYYY-MM-DDTHH:mm:ssZ)"
                );
            }
        }
    }

    public static String validateAndFormatTable(String exportTable) {
        if (exportTable == null || exportTable.isBlank()) {
            throw new IllegalArgumentException("Property 'exportTable' must not be empty");
        }
        var raw = exportTable.trim();
        if (raw.startsWith("`") && raw.endsWith("`")) {
            raw = raw.substring(1, raw.length() - 1);
        }
        if (!TABLE_PATTERN.matcher(raw).matches()) {
            throw new IllegalArgumentException(
                "Invalid BigQuery table name '" + exportTable + "'. Must match pattern: ^[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+){1,2}$"
            );
        }
        return "`" + raw + "`";
    }

    public static String buildSql(
        String exportTable,
        String dateFrom,
        String dateTo,
        List<String> groupBy,
        String filter,
        boolean includeCredits
    ) {
        var cleanTable = validateAndFormatTable(exportTable);
        var canonicalDateFrom = parseCanonicalTimestamp(dateFrom, "dateFrom");

        var selectParts = new ArrayList<String>();
        var groupByParts = new ArrayList<String>();

        if (groupBy != null && !groupBy.isEmpty()) {
            for (var col : groupBy) {
                var trimmed = col.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                var upper = trimmed.toUpperCase();
                if (upper.contains(" AS ")) {
                    var asIndex = upper.indexOf(" AS ");
                    var expr = trimmed.substring(0, asIndex).trim();
                    selectParts.add(trimmed);
                    groupByParts.add(expr);
                } else if (trimmed.contains(".")) {
                    var alias = trimmed.replace(".", "_");
                    selectParts.add(trimmed + " AS " + alias);
                    groupByParts.add(trimmed);
                } else {
                    selectParts.add(trimmed);
                    groupByParts.add(trimmed);
                }
            }
        }

        selectParts.add("currency");
        groupByParts.add("currency");

        selectParts.add("SUM(cost) AS total_cost");
        if (includeCredits) {
            selectParts.add("SUM(cost) + IFNULL(SUM((SELECT SUM(c.amount) FROM UNNEST(credits) c)), 0) AS net_cost");
        }

        var whereParts = new ArrayList<String>();
        whereParts.add("usage_start_time >= TIMESTAMP('" + canonicalDateFrom + "')");
        if (dateTo != null && !dateTo.trim().isEmpty()) {
            var canonicalDateTo = parseCanonicalTimestamp(dateTo, "dateTo");
            whereParts.add("usage_start_time < TIMESTAMP('" + canonicalDateTo + "')");
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

        var rExportTable = runContext.render(this.getExportTable()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'exportTable'"));
        var rDateFrom = runContext.render(this.getDateFrom()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'dateFrom'"));
        var rDateTo = runContext.render(this.getDateTo()).as(String.class).orElse(null);
        var rGroupBy = runContext.render(this.getGroupBy()).asList(String.class);
        var rFilter = runContext.render(this.getFilter()).as(String.class).orElse(null);
        var rIncludeCredits = runContext.render(this.getIncludeCredits()).as(Boolean.class).orElse(true);

        var generatedSql = buildSql(
            rExportTable,
            rDateFrom,
            rDateTo,
            rGroupBy,
            rFilter,
            rIncludeCredits
        );

        logger.debug("Executing generated BigQuery billing export query:\n{}", generatedSql);

        var internalQuery = Query.builder()
            .id(this.getId() + "_query")
            .type(Query.class.getName())
            .projectId(this.getProjectId())
            .serviceAccount(this.getServiceAccount())
            .impersonatedServiceAccount(this.getImpersonatedServiceAccount())
            .scopes(this.getScopes())
            .sql(Property.ofValue(generatedSql))
            .fetchType(this.getFetchType())
            .destinationTable(this.getDestinationTable())
            .build();

        this.queryTask.set(internalQuery);
        Query.Output queryOutput;
        try {
            queryOutput = executeQuery(runContext, internalQuery);
        } finally {
            this.queryTask.set(null);
        }

        return Output.builder()
            .rows(queryOutput.getRows())
            .row(queryOutput.getRow())
            .uri(queryOutput.getUri())
            .size(queryOutput.getSize())
            .jobId(queryOutput.getJobId())
            .sql(generatedSql)
            .build();
    }

    Query.Output executeQuery(RunContext runContext, Query query) throws Exception {
        return query.run(runContext);
    }

    @Override
    public void kill() {
        var task = this.queryTask.get();
        if (task != null) {
            task.kill();
        }
    }

    @Override
    public void stop() {
        var task = this.queryTask.get();
        if (task != null) {
            task.stop();
        }
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
