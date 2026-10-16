package io.kestra.plugin.gcp.billing;

import java.io.InputStream;
import java.net.URI;
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
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.gcp.billing.models.BudgetAlert;
import io.kestra.plugin.gcp.pubsub.Consume;
import io.kestra.plugin.gcp.pubsub.PubSubConnectionInterface;
import io.kestra.plugin.gcp.pubsub.model.Message;
import io.kestra.plugin.gcp.pubsub.model.SerdeType;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
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
            title = "Trigger a flow when a budget exceeds 85% of its allocation",
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
                    serviceAccount: "{{ secret('GCP_SA') }}"
                    topic: "gcp-budget-alerts"
                    subscription: "kestra-budget-alerts-sub"
                    thresholdRatio: 0.85
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

    @Builder.Default
    @Schema(title = "The GCP scopes to be used")
    @PluginProperty(group = "connection")
    protected Property<List<String>> scopes = Property.ofValue(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

    @NotNull
    @Schema(
        title = "Topic ID",
        description = "The Pub/Sub topic name receiving Cloud Billing budget notifications."
    )
    @PluginProperty(group = "main")
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

    @DecimalMin("0.0")
    @Schema(
        title = "Threshold ratio (between 0.0 and 1.0+)",
        description = "Minimum budget usage ratio required to trigger the flow (e.g. `0.9` for 90%, `1.0` for 100%). If unset, any notification that crosses a Google budget threshold will trigger."
    )
    @PluginProperty(group = "processing")
    private Property<Double> thresholdRatio;

    @Deprecated
    @Schema(
        title = "Threshold percentage",
        description = "Deprecated: use `thresholdRatio` instead."
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

    @Builder.Default
    @Schema(title = "Max records per poll", description = "Maximum number of messages to consume per polling run")
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords = Property.ofValue(10);

    @Builder.Default
    @Schema(title = "Max duration", description = "Duration limit for a polling run; defaults to PT30S")
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(30));

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Consume> consumeTask = new AtomicReference<>();

    public static BudgetAlert parseAlertPayload(String payload, Map<String, String> attributes) {
        return parseAlertPayload(payload, attributes, "unknown", null);
    }

    public static BudgetAlert parseAlertPayload(String payload, Map<String, String> attributes, String messageId, org.slf4j.Logger logger) {
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

            var alertThresholdExceeded = map.get("alertThresholdExceeded") != null
                ? Double.parseDouble(map.get("alertThresholdExceeded").toString())
                : (Double) null;
            var forecastThresholdExceeded = map.get("forecastThresholdExceeded") != null
                ? Double.parseDouble(map.get("forecastThresholdExceeded").toString())
                : (Double) null;

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
                .alertThresholdExceeded(alertThresholdExceeded)
                .forecastThresholdExceeded(forecastThresholdExceeded)
                .billingAccountId(billingAccountId)
                .budgetId(budgetId)
                .build();
        } catch (Exception e) {
            if (logger != null) {
                logger.warn("Failed to parse budget alert notification (messageId: '{}'): {}", messageId, e.getMessage());
            }
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
            .maxDuration(this.maxDuration)
            .serdeType(Property.ofValue(SerdeType.STRING))
            .build();

        this.trackTask(consume);

        Consume.Output run;
        try {
            run = consume.run(runContext);
        } finally {
            this.clearTask(consume);
        }

        if (run.getCount() == 0 || run.getUri() == null) {
            return Optional.empty();
        }

        var rThresholdRatio = runContext.render(this.thresholdRatio != null ? this.thresholdRatio : this.thresholdPercent)
            .as(Double.class).orElse(null);
        var rThresholdAmount = runContext.render(this.thresholdAmount).as(Double.class).orElse(null);

        var matchedAlerts = findMatchingAlerts(runContext, context, run.getUri(), rThresholdRatio, rThresholdAmount, logger);
        if (matchedAlerts.isEmpty()) {
            return Optional.empty();
        }

        var output = Output.builder()
            .budgetAlert(matchedAlerts.get(0))
            .budgetAlerts(matchedAlerts)
            .build();

        var execution = TriggerService.generateExecution(this, conditionContext, context, output);
        return Optional.of(execution);
    }

    private List<BudgetAlert> findMatchingAlerts(
        RunContext runContext,
        TriggerContext context,
        URI fileUri,
        Double thresholdRatio,
        Double thresholdAmount,
        org.slf4j.Logger logger
    ) throws Exception {
        var matched = new ArrayList<BudgetAlert>();
        try (var is = runContext.storage().getFile(fileUri)) {
            var messages = FileSerde.readAll(is, Message.class).collectList().block();
            if (messages != null) {
                for (var msg : messages) {
                    var dataStr = msg.getData() != null ? msg.getData().toString() : null;
                    var messageId = msg.getMessageId() != null ? msg.getMessageId() : "unknown";
                    var alert = parseAlertPayload(dataStr, msg.getAttributes(), messageId, logger);
                    if (alert == null) {
                        continue;
                    }

                    if (shouldFireAndRecordState(runContext, context, alert, thresholdRatio, thresholdAmount, logger)) {
                        logger.info(
                            "Budget alert triggered for '{}': cost {} exceeds threshold",
                            alert.getBudgetDisplayName(),
                            alert.getCostAmount()
                        );
                        matched.add(alert.toBuilder().thresholdCrossed(true).build());
                    }
                }
            }
        } finally {
            try {
                runContext.storage().deleteFile(fileUri);
            } catch (Exception e) {
                logger.warn("Failed to delete internal storage file '{}': {}", fileUri, e.getMessage());
            }
        }
        return matched;
    }

    private boolean shouldFireAndRecordState(
        RunContext runContext,
        TriggerContext context,
        BudgetAlert alert,
        Double thresholdRatio,
        Double thresholdAmount,
        org.slf4j.Logger logger
    ) {
        var passesRatio = thresholdRatio == null || alert.getPercentageUsed() >= thresholdRatio;
        var passesAmount = thresholdAmount == null || alert.getCostAmount() >= thresholdAmount;

        if (!passesRatio || !passesAmount) {
            logger.debug(
                "Budget snapshot for '{}' (cost: {}) did not cross threshold (ratio: {}, amount: {})",
                alert.getBudgetDisplayName(),
                alert.getCostAmount(),
                thresholdRatio,
                thresholdAmount
            );
            return false;
        }

        var billingAccount = alert.getBillingAccountId() != null ? alert.getBillingAccountId() : "default";
        var budgetId = alert.getBudgetId() != null ? alert.getBudgetId() : (alert.getBudgetDisplayName() != null ? alert.getBudgetDisplayName() : "default");
        var intervalStart = alert.getCostIntervalStart() != null ? alert.getCostIntervalStart() : "default";

        var sanitizedBilling = billingAccount.replaceAll("[^a-zA-Z0-9_-]", "_");
        var sanitizedBudget = budgetId.replaceAll("[^a-zA-Z0-9_-]", "_");
        var sanitizedInterval = intervalStart.replaceAll("[^a-zA-Z0-9_-]", "_");

        var kvKey = ("budget_alert_" + context.getFlowId() + "_" + this.getId() + "_" + sanitizedBilling + "_" + sanitizedBudget + "_" + sanitizedInterval)
            .toLowerCase();

        var currentThreshold = alert.getAlertThresholdExceeded() != null
            ? alert.getAlertThresholdExceeded()
            : alert.getPercentageUsed();

        try {
            var kv = runContext.namespaceKv(context.getNamespace());
            var entry = kv.getValue(kvKey);
            if (entry != null && entry.isPresent() && entry.get().value() != null) {
                var prevThreshold = Double.parseDouble(entry.get().value().toString());
                if (currentThreshold <= prevThreshold) {
                    logger.debug(
                        "Deduplicated budget alert for '{}': snapshot threshold {} <= previously fired threshold {}",
                        alert.getBudgetDisplayName(),
                        currentThreshold,
                        prevThreshold
                    );
                    return false;
                }
            }

            kv.put(kvKey, new KVValueAndMetadata(null, currentThreshold));
        } catch (Exception e) {
            logger.debug("Namespace KV store access for key '{}': {}", kvKey, e.getMessage());
        }

        return true;
    }

    void trackTask(Consume task) {
        this.consumeTask.set(task);
    }

    void clearTask(Consume task) {
        this.consumeTask.compareAndSet(task, null);
    }

    @Override
    public void kill() {
        var task = this.consumeTask.get();
        if (task != null) {
            task.kill();
        }
    }

    @Override
    public void stop() {
        var task = this.consumeTask.get();
        if (task != null) {
            task.stop();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The primary or first matched budget alert")
        private final BudgetAlert budgetAlert;

        @Schema(title = "All budget alerts that matched in this polling run")
        private final List<BudgetAlert> budgetAlerts;
    }
}
