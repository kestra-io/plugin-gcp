package io.kestra.plugin.gcp.billing.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class BudgetAlert {
    @Schema(title = "The display name of the budget that generated this alert")
    String budgetDisplayName;

    @Schema(title = "The current cost amount in the budget period")
    Double costAmount;

    @Schema(title = "The total budget amount")
    Double budgetAmount;

    @Schema(title = "The start timestamp of the budget interval")
    String costIntervalStart;

    @Schema(title = "The type of budget amount (e.g. SPECIFIED_AMOUNT)")
    String budgetAmountType;

    @Schema(title = "The currency code for the cost and budget amounts")
    String currencyCode;

    @Schema(title = "The ratio of current cost to total budget amount (costAmount / budgetAmount)")
    Double percentageUsed;

    @Schema(title = "Whether this snapshot has crossed the configured alert threshold")
    Boolean thresholdCrossed;

    @Schema(title = "The billing account ID associated with the budget alert")
    String billingAccountId;

    @Schema(title = "The budget ID associated with the alert")
    String budgetId;
}
