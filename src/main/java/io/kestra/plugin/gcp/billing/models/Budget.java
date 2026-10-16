package io.kestra.plugin.gcp.billing.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class Budget {
    @Schema(title = "The resource name of the budget", description = "Format: billingAccounts/{billingAccountId}/budgets/{budgetId}")
    String name;

    @Schema(title = "The display name of the budget")
    String displayName;

    @Schema(title = "The specified budget amount in currency units")
    Double specifiedAmount;

    @Schema(title = "Currency code of the budget amount")
    String currencyCode;

    @Schema(title = "List of threshold rules defining alert percentages")
    List<Double> thresholdPercentRules;

    public static Budget of(com.google.cloud.billing.budgets.v1.Budget budget) {
        var thresholds = budget.getThresholdRulesList().stream()
            .map(com.google.cloud.billing.budgets.v1.ThresholdRule::getThresholdPercent)
            .toList();

        var builder = Budget.builder()
            .name(budget.getName())
            .displayName(budget.getDisplayName())
            .thresholdPercentRules(thresholds);

        if (budget.hasAmount() && budget.getAmount().hasSpecifiedAmount()) {
            var money = budget.getAmount().getSpecifiedAmount();
            var amount = money.getUnits() + (money.getNanos() / 1_000_000_000.0);
            builder.specifiedAmount(amount)
                .currencyCode(money.getCurrencyCode());
        }

        return builder.build();
    }
}
