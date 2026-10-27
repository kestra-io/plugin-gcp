package io.kestra.plugin.gcp.billing;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.gcp.pubsub.Consume;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@KestraTest
class BudgetAlertTriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    private URI createMessageFile(RunContext runContext, String payload, Map<String, String> attributes, String messageId) throws Exception {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var os = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            var msg = Message.of(
                PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(payload))
                    .putAllAttributes(attributes)
                    .setMessageId(messageId)
                    .build(),
                SerdeType.STRING
            );
            FileSerde.write(os, msg);
        }
        return runContext.storage().putFile(tempFile);
    }

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

        var alert = BudgetAlertTrigger.parseAlertPayload(payload, attributes, "msg-1", null);

        assertThat(alert, notNullValue());
        assertThat(alert.getBudgetDisplayName(), is("Monthly Spending Limit"));
        assertThat(alert.getCostAmount(), is(95.50));
        assertThat(alert.getBudgetAmount(), is(100.00));
        assertThat(alert.getCurrencyCode(), is("USD"));
        assertThat(alert.getUsageRatio(), closeTo(0.955, 0.001));
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
            .id("budget_trigger_" + IdUtils.create())
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

        var rTopic = runContext.render(trigger.getTopic()).as(String.class).orElseThrow();
        var rSub = runContext.render(trigger.getSubscription()).as(String.class).orElseThrow();
        var rRatio = runContext.render(trigger.getThresholdRatio()).as(Double.class).orElseThrow();
        var rAmount = runContext.render(trigger.getThresholdAmount()).as(Double.class).orElseThrow();

        assertThat(rTopic, is("gcp-budget-alerts"));
        assertThat(rSub, is("kestra-budget-alerts-sub"));
        assertThat(rRatio, is(0.9));
        assertThat(rAmount, is(500.0));
    }

    @Test
    void shouldEvaluateAndDeduplicateBudgetAlerts() throws Exception {
        var trigger = spy(BudgetAlertTrigger.builder()
            .id("budget_trigger_eval_" + IdUtils.create())
            .type(BudgetAlertTrigger.class.getName())
            .topic(Property.ofValue("gcp-budget-alerts"))
            .subscription(Property.ofValue("sub-alerts"))
            .thresholdRatio(Property.ofValue(0.9))
            .build());

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = context.getKey().getRunContext();

        var payload90 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 92.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 100.00,
              "currencyCode": "USD",
              "alertThresholdExceeded": 0.9
            }
            """;
        var attributes = Map.of(
            "billingAccountId", "0123AB-4567CD-89EF01",
            "budgetId", "my-monthly-budget"
        );

        // 1. First poll with 90% threshold alert: should trigger execution
        var uri1 = createMessageFile(runContext, payload90, attributes, "msg-001");
        doReturn(Consume.Output.builder().count(1).uri(uri1).build()).when(trigger).runConsume(any(), any());

        var execution1 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution1.isPresent(), is(true));
        assertThat(execution1.get().getTrigger().getVariables().get("budgetAlert"), notNullValue());
        assertThat(execution1.get().getTrigger().getVariables().get("budgetAlerts"), notNullValue());

        // 2. Second poll with same snapshot: should be suppressed by KV deduplication
        var uri2 = createMessageFile(runContext, payload90, attributes, "msg-002");
        doReturn(Consume.Output.builder().count(1).uri(uri2).build()).when(trigger).runConsume(any(), any());

        var execution2 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution2.isEmpty(), is(true));

        // 3. Third poll with higher threshold (100% threshold exceeded): should trigger
        var payload100 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 102.50,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 100.00,
              "currencyCode": "USD",
              "alertThresholdExceeded": 1.0
            }
            """;
        var uri3 = createMessageFile(runContext, payload100, attributes, "msg-003");
        doReturn(Consume.Output.builder().count(1).uri(uri3).build()).when(trigger).runConsume(any(), any());

        var execution3 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution3.isPresent(), is(true));
    }

    @Test
    void shouldIgnorePeriodicSnapshotWhenNoThresholdExceededByDefault() throws Exception {
        var trigger = spy(BudgetAlertTrigger.builder()
            .id("budget_trigger_default_" + IdUtils.create())
            .type(BudgetAlertTrigger.class.getName())
            .topic(Property.ofValue("gcp-budget-alerts"))
            .subscription(Property.ofValue("sub-alerts"))
            .build());

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = context.getKey().getRunContext();

        // Periodic update without alertThresholdExceeded
        var periodicPayload = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 45.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 100.00,
              "currencyCode": "USD"
            }
            """;
        var uri = createMessageFile(runContext, periodicPayload, Map.of("billingAccountId", "0123AB", "budgetId", "b-1"), "msg-periodic");
        doReturn(Consume.Output.builder().count(1).uri(uri).build()).when(trigger).runConsume(any(), any());

        var execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isEmpty(), is(true));
    }

    @Test
    void shouldNotSuppressGoogleAlertsWhenThresholdAmountIsConfigured() throws Exception {
        var trigger = spy(BudgetAlertTrigger.builder()
            .id("budget_trigger_amount_" + IdUtils.create())
            .type(BudgetAlertTrigger.class.getName())
            .topic(Property.ofValue("gcp-budget-alerts"))
            .subscription(Property.ofValue("sub-alerts"))
            .thresholdAmount(Property.ofValue(500.0))
            .build());

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var runContext = context.getKey().getRunContext();

        var attributes = Map.of(
            "billingAccountId", "0123AB-4567CD-89EF01",
            "budgetId", "my-monthly-budget"
        );

        // 1. Initial alert crossing thresholdAmount ($500 out of $1000 budget): fires (stores ratio 0.5)
        var payload500 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 500.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 1000.00,
              "currencyCode": "USD"
            }
            """;
        var uri1 = createMessageFile(runContext, payload500, attributes, "msg-amt-1");
        doReturn(Consume.Output.builder().count(1).uri(uri1).build()).when(trigger).runConsume(any(), any());
        var execution1 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution1.isPresent(), is(true));

        // 2. Periodic snapshot without Google threshold: deduplicated (0.5 <= 0.5)
        var payload520 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 520.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 1000.00,
              "currencyCode": "USD"
            }
            """;
        var uri2 = createMessageFile(runContext, payload520, attributes, "msg-amt-2");
        doReturn(Consume.Output.builder().count(1).uri(uri2).build()).when(trigger).runConsume(any(), any());
        var execution2 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution2.isEmpty(), is(true));

        // 3. Subsequent Google 90% threshold alert: MUST NOT be suppressed by thresholdAmount
        var payload900 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 900.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 1000.00,
              "currencyCode": "USD",
              "alertThresholdExceeded": 0.9
            }
            """;
        var uri3 = createMessageFile(runContext, payload900, attributes, "msg-amt-3");
        doReturn(Consume.Output.builder().count(1).uri(uri3).build()).when(trigger).runConsume(any(), any());
        var execution3 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution3.isPresent(), is(true));

        // 4. Subsequent Google 100% threshold alert: MUST NOT be suppressed
        var payload1000 = """
            {
              "budgetDisplayName": "Monthly Spending Limit",
              "costAmount": 1000.00,
              "costIntervalStart": "2026-10-01T00:00:00Z",
              "budgetAmount": 1000.00,
              "currencyCode": "USD",
              "alertThresholdExceeded": 1.0
            }
            """;
        var uri4 = createMessageFile(runContext, payload1000, attributes, "msg-amt-4");
        doReturn(Consume.Output.builder().count(1).uri(uri4).build()).when(trigger).runConsume(any(), any());
        var execution4 = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution4.isPresent(), is(true));
    }
}
