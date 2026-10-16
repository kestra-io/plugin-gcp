package io.kestra.plugin.gcp.billing;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class BillingExportQueryTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldBuildSqlWithGroupByAndCredits() {
        var sql = BillingExportQuery.buildSql(
            "billing.gcp_billing_export_v1_0123AB_4567CD_89EF01",
            "2026-09-01",
            "2026-10-01",
            List.of("service.description", "project.id"),
            "cost > 0",
            true
        );

        assertThat(sql, containsString("SELECT"));
        assertThat(sql, containsString("service.description AS service_description"));
        assertThat(sql, containsString("project.id AS project_id"));
        assertThat(sql, containsString("SUM(cost) AS total_cost"));
        assertThat(sql, containsString("SUM(cost) + IFNULL(SUM((SELECT SUM(c.amount) FROM UNNEST(credits) c)), 0) AS net_cost"));
        assertThat(sql, containsString("FROM `billing.gcp_billing_export_v1_0123AB_4567CD_89EF01`"));
        assertThat(sql, containsString("usage_start_time >= TIMESTAMP('2026-09-01T00:00:00Z')"));
        assertThat(sql, containsString("usage_start_time < TIMESTAMP('2026-10-01T00:00:00Z')"));
        assertThat(sql, containsString("(cost > 0)"));
        assertThat(sql, containsString("GROUP BY service.description, project.id, currency"));
        assertThat(sql, containsString("ORDER BY net_cost DESC"));
    }

    @Test
    void shouldBuildSqlWithoutCreditsAndSingleDate() {
        var sql = BillingExportQuery.buildSql(
            "`my-project.billing_dataset.gcp_billing_export_v1_XXXXXX`",
            "2026-10-01T00:00:00Z",
            null,
            List.of("sku.description"),
            null,
            false
        );

        assertThat(sql, containsString("SELECT"));
        assertThat(sql, containsString("sku.description AS sku_description"));
        assertThat(sql, containsString("SUM(cost) AS total_cost"));
        assertThat(sql, not(containsString("net_cost")));
        assertThat(sql, containsString("FROM `my-project.billing_dataset.gcp_billing_export_v1_XXXXXX`"));
        assertThat(sql, containsString("usage_start_time >= TIMESTAMP('2026-10-01T00:00:00Z')"));
        assertThat(sql, not(containsString("usage_start_time <")));
        assertThat(sql, containsString("ORDER BY total_cost DESC"));
    }

    @Test
    void shouldFailOnInvalidTableOrDate() {
        assertThrows(IllegalArgumentException.class, () ->
            BillingExportQuery.buildSql("invalid table with spaces;", "2026-09-01", null, null, null, true)
        );

        assertThrows(IllegalArgumentException.class, () ->
            BillingExportQuery.buildSql("dataset.table", "invalid-date", null, null, null, true)
        );
    }

    @Test
    void shouldRenderPropertiesWithTemplateExpressions() throws Exception {
        var task = BillingExportQuery.builder()
            .id("billing_query")
            .type(BillingExportQuery.class.getName())
            .exportTable(Property.ofExpression("{{ export_tbl }}"))
            .dateFrom(Property.ofExpression("{{ date_start }}"))
            .groupBy(Property.ofValue(List.of("service.description")))
            .build();

        var runContext = runContextFactory.of(Map.of(
            "export_tbl", "billing.export_table_test",
            "date_start", "2026-09-15"
        ));

        var renderedTable = runContext.render(task.getExportTable()).as(String.class).orElseThrow();
        var renderedDate = runContext.render(task.getDateFrom()).as(String.class).orElseThrow();

        assertThat(renderedTable, is("billing.export_table_test"));
        assertThat(renderedDate, is("2026-09-15"));
    }
}
