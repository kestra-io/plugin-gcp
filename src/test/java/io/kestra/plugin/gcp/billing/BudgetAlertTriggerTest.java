package io.kestra.plugin.gcp.billing;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class BudgetAlertTriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldParseValidBudgetAlertPayload() {
        var payload = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 95.50,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 100.00,
              "budgetAmountType": "SPECIFIED_AMOUNT",
              "currencyCode": "USD",
              "alertThresholdExceeded": 0.9,
              "forecastThresholdExceeded": 1.0
            }
            """;

        var attributes = Map.of(
            "billingAccountId", "0123AB-4567CD-89EF01",
            "budgetId", "my-monthly-budget"
        );

        var alert = BudgetAlertTrigger.parseAlertPayload(payload, attributes);

        assertThat(alert, notNullValue());
        assertThat(alert.getBudgetDisplayName(), is("Monthly Spending Limit"));
        assertThat(alert.getCostAmount(), is(95.50));
        assertThat(alert.getBudgetAmount(), is(100.00));
        assertThat(alert.getCurrencyCode(), is("USD"));
        assertThat(alert.getPercentageUsed(), closeTo(0.955, 0.001));
        assertThat(alert.getAlertThresholdExceeded(), is(0.9));
        assertThat(alert.getForecastThresholdExceeded(), is(1.0));
        assertThat(alert.getBillingAccountId(), is("0123AB-4567CD-89EF01"));
        assertThat(alert.getBudgetId(), is("my-monthly-budget"));
    }

    @Test
    void shouldReturnNullAndLogOnInvalidPayload() {
        var alert = BudgetAlertTrigger.parseAlertPayload("invalid-json", Map.of(), "msg-123", null);
        assertThat(alert, nullValue());
    }

    @Test
    void shouldRenderTriggerPropertiesCorrectly() throws Exception {
        var trigger = BudgetAlertTrigger.builder()
            .id("budget_trigger")
            .type(BudgetAlertTrigger.class.getName())
            .topic(Property.ofExpression("{{ topic_name }}"))
            .subscription(Property.ofExpression("{{ sub_name }}"))
            .thresholdRatio(Property.ofValue(0.9))
            .thresholdAmount(Property.ofValue(500.0))
            .build();

        var runContext = runContextFactory.of(Map.of(
            "topic_name", "gcp-budget-alerts",
            "sub_name", "kestra-budget-alerts-sub"
        ));

        var renderedTopic = runContext.render(trigger.getTopic()).as(String.class).orElseThrow();
        var renderedSub = runContext.render(trigger.getSubscription()).as(String.class).orElseThrow();
        var renderedRatio = runContext.render(trigger.getThresholdRatio()).as(Double.class).orElseThrow();
        var renderedAmount = runContext.render(trigger.getThresholdAmount()).as(Double.class).orElseThrow();

        assertThat(renderedTopic, is("gcp-budget-alerts"));
        assertThat(renderedSub, is("kestra-budget-alerts-sub"));
        assertThat(renderedRatio, is(0.9));
        assertThat(renderedAmount, is(500.0));
    }
}
