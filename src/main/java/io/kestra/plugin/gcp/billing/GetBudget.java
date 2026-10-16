package io.kestra.plugin.gcp.billing;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.billing.models.Budget;

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
    title = "Get a GCP Cloud Billing Budget",
    description = "Retrieves a specific Cloud Billing budget definition, including thresholds and notification rules."
)
@Plugin(
    examples = {
        @Example(
            title = "Get budget details by resource name",
            full = true,
            code = """
                id: gcp_get_budget
                namespace: company.finops

                tasks:
                  - id: get_budget
                    type: io.kestra.plugin.gcp.billing.GetBudget
                    projectId: "{{ secret('GCP_PROJECT') }}"
                    serviceAccount: "{{ secret('GCP_SA') }}"
                    budgetName: "billingAccounts/0123AB-4567CD-89EF01/budgets/my-monthly-budget"
                """
        )
    }
)
public class GetBudget extends AbstractBilling implements RunnableTask<GetBudget.Output> {

    @NotNull
    @Schema(
        title = "The resource name of the budget",
        description = "Format: `billingAccounts/{billingAccountId}/budgets/{budgetId}`"
    )
    @PluginProperty(group = "source")
    private Property<String> budgetName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var renderedBudgetName = runContext.render(this.budgetName).as(String.class).orElseThrow();
        logger.debug("Fetching budget: {}", renderedBudgetName);

        Budget budget;
        try (var client = this.budgetServiceClient(runContext)) {
            var response = client.getBudget(renderedBudgetName);
            budget = Budget.of(response);
        }

        logger.info(
            "Retrieved budget '{}' with specified amount {} {}",
            budget.getDisplayName(),
            budget.getSpecifiedAmount(),
            budget.getCurrencyCode()
        );

        return Output.builder()
            .budget(budget)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The budget details")
        private final Budget budget;
    }
}
