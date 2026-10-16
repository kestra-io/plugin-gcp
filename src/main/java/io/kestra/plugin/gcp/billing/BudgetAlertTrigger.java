package io.kestra.plugin.gcp.billing;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.gcp.billing.models.BudgetAlert;
import io.kestra.plugin.gcp.pubsub.Consume;
import io.kestra.plugin.gcp.pubsub.PubSubConnectionInterface;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;

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
    title = "Trigger on GCP Cloud Billing Budget Alerts",
    description = "Consumes budget notifications published to a Google Cloud Pub/Sub topic and starts a Flow when the cost crosses the configured threshold."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a budget exceeds 90% of its allocation",
            full = true,
            code = """
                id: gcp_budget_alert_listener
                namespace: company.finops

                tasks:
                  - id: notify_slack
                    type: io.kestra.plugin.core.log.Log
                    message: "ALERT: Budget '{{ trigger.budgetAlert.budgetDisplayName }}' reached {{ trigger.budgetAlert.percentageUsed * 100 }}% (Cost: {{ trigger.budgetAlert.costAmount }} {{ trigger.budgetAlert.currencyCode }})"

                triggers:
                  - id: budget_alert
                    type: io.kestra.plugin.gcp.billing.BudgetAlertTrigger
                    projectId: "{{ secret('GCP_PROJECT') }}"
                    serviceAccount: "{{ secret('GCP_SA') }}"
                    topic: "gcp-budget-alerts"
                    subscription: "kestra-budget-alerts-sub"
                    thresholdPercent: 0.9
                """
        )
    }
)
public class BudgetAlertTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<BudgetAlertTrigger.Output>, PubSubConnectionInterface {

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

    @Schema(
        title = "Topic ID",
        description = "The Pub/Sub topic name receiving Cloud Billing budget notifications."
    )
    @PluginProperty(group = "source")
    private Property<String> topic;

    @NotNull
    @Schema(
        title = "Subscription ID",
        description = "The Pub/Sub subscription name to pull budget notifications from."
    )
    @PluginProperty(group = "source")
    private Property<String> subscription;

    @Builder.Default
    @Schema(
        title = "Auto-create subscription",
        description = "Whether to create the subscription if it does not already exist."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoCreateSubscription = Property.ofValue(true);

    @Schema(
        title = "Threshold percentage (ratio between 0.0 and 1.0+)",
        description = "Minimum budget usage ratio required to trigger the flow (e.g. `0.9` for 90%, `1.0` for 100%)."
    )
    @PluginProperty(group = "processing")
    private Property<Double> thresholdPercent;

    @Schema(
        title = "Threshold amount",
        description = "Minimum absolute cost amount required to trigger the flow."
    )
    @PluginProperty(group = "processing")
    private Property<Double> thresholdAmount;

    @Builder.Default
    @Schema(title = "The interval between polling iterations")
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Max records per poll", description = "Maximum number of messages to consume per polling run")
    @PluginProperty(group = "execution")
    @Builder.Default
    private Property<Integer> maxRecords = Property.ofValue(10);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Consume> consumeTask = new AtomicReference<>();

    public static BudgetAlert parseAlertPayload(String payload, Map<String, String> attributes) {
        if (payload == null || payload.isBlank()) {
            return null;
        }

        try {
            var mapper = JacksonMapper.ofJson();
            var map = mapper.readValue(payload, new TypeReference<Map<String, Object>>() {});

            var budgetDisplayName = (String) map.get("budgetDisplayName");
            var costAmount = map.get("costAmount") != null ? Double.parseDouble(map.get("costAmount").toString()) : 0.0;
            var budgetAmount = map.get("budgetAmount") != null ? Double.parseDouble(map.get("budgetAmount").toString()) : 0.0;
            var costIntervalStart = (String) map.get("costIntervalStart");
            var budgetAmountType = (String) map.get("budgetAmountType");
            var currencyCode = (String) map.get("currencyCode");

            var percentageUsed = budgetAmount > 0 ? (costAmount / budgetAmount) : 0.0;

            var billingAccountId = attributes != null ? attributes.get("billingAccountId") : null;
            var budgetId = attributes != null ? attributes.get("budgetId") : null;

            return BudgetAlert.builder()
                .budgetDisplayName(budgetDisplayName)
                .costAmount(costAmount)
                .budgetAmount(budgetAmount)
                .costIntervalStart(costIntervalStart)
                .budgetAmountType(budgetAmountType)
                .currencyCode(currencyCode)
                .percentageUsed(percentageUsed)
                .billingAccountId(billingAccountId)
                .budgetId(budgetId)
                .build();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var consume = Consume.builder()
            .topic(this.topic)
            .subscription(this.subscription)
            .autoCreateSubscription(this.autoCreateSubscription)
            .projectId(this.projectId)
            .serviceAccount(this.serviceAccount)
            .impersonatedServiceAccount(this.impersonatedServiceAccount)
            .scopes(this.scopes)
            .maxRecords(this.maxRecords)
            .serdeType(Property.ofValue(SerdeType.STRING))
            .build();

        this.consumeTask.set(consume);

        Consume.Output run;
        try {
            run = consume.run(runContext);
        } finally {
            this.consumeTask.set(null);
        }

        if (run.getCount() == 0 || run.getUri() == null) {
            return Optional.empty();
        }

        var rThresholdPercent = runContext.render(this.thresholdPercent).as(Double.class).orElse(null);
        var rThresholdAmount = runContext.render(this.thresholdAmount).as(Double.class).orElse(null);

        var matchedAlert = findMatchingAlert(runContext, run.getUri(), rThresholdPercent, rThresholdAmount, logger);
        if (matchedAlert == null) {
            return Optional.empty();
        }

        var output = Output.builder()
            .budgetAlert(matchedAlert)
            .build();

        var execution = TriggerService.generateExecution(this, conditionContext, context, output);
        return Optional.of(execution);
    }

    private BudgetAlert findMatchingAlert(RunContext runContext, java.net.URI fileUri, Double thresholdPercent, Double thresholdAmount, org.slf4j.Logger logger) throws Exception {
        try (var is = runContext.storage().getFile(fileUri)) {
            var messages = FileSerde.readAll(is, Message.class).collectList().block();
            if (messages == null) {
                return null;
            }

            for (var msg : messages) {
                var dataStr = msg.getData() != null ? msg.getData().toString() : null;
                var alert = parseAlertPayload(dataStr, msg.getAttributes());
                if (alert == null) {
                    continue;
                }

                var crossesPercent = thresholdPercent == null || alert.getPercentageUsed() >= thresholdPercent;
                var crossesAmount = thresholdAmount == null || alert.getCostAmount() >= thresholdAmount;

                if (crossesPercent && crossesAmount) {
                    logger.info(
                        "Budget alert triggered for '{}': cost {} exceeds threshold",
                        alert.getBudgetDisplayName(),
                        alert.getCostAmount()
                    );
                    return alert.toBuilder().thresholdCrossed(true).build();
                } else {
                    logger.debug(
                        "Budget snapshot for '{}' (cost: {}) did not cross threshold (percent: {}, amount: {})",
                        alert.getBudgetDisplayName(),
                        alert.getCostAmount(),
                        thresholdPercent,
                        thresholdAmount
                    );
                }
            }
        }
        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The matched budget alert details")
        private final BudgetAlert budgetAlert;
    }
}
