package io.kestra.plugin.gcp.billing;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.cloud.billing.budgets.v1.BudgetAmount;
import com.google.cloud.billing.budgets.v1.ThresholdRule;
import com.google.type.Money;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.gcp.billing.models.Budget;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class GetBudgetTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldBuildAndRenderBudgetNameProperty() throws Exception {
        var task = GetBudget.builder()
            .id("get_budget_" + IdUtils.create())
            .type(GetBudget.class.getName())
            .budgetName(Property.ofExpression("{{ budget_name_var }}"))
            .build();

        var runContext = runContextFactory.of(Map.of(
            "budget_name_var", "billingAccounts/0123AB-4567CD-89EF01/budgets/my-budget"
        ));

        var rBudgetName = runContext.render(task.getBudgetName()).as(String.class).orElseThrow();
        assertThat(rBudgetName, is("billingAccounts/0123AB-4567CD-89EF01/budgets/my-budget"));
    }

    @Test
    void shouldMapBudgetFromProto() {
        var money = Money.newBuilder()
            .setCurrencyCode("USD")
            .setUnits(250)
            .setNanos(500_000_000)
            .build();

        var proto = com.google.cloud.billing.budgets.v1.Budget.newBuilder()
            .setName("billingAccounts/0123AB-4567CD-89EF01/budgets/my-budget")
            .setDisplayName("Monthly Analytics Budget")
            .setAmount(BudgetAmount.newBuilder().setSpecifiedAmount(money).build())
            .addThresholdRules(ThresholdRule.newBuilder().setThresholdPercent(0.5).build())
            .addThresholdRules(ThresholdRule.newBuilder().setThresholdPercent(0.9).build())
            .addThresholdRules(ThresholdRule.newBuilder().setThresholdPercent(1.0).build())
            .build();

        var model = Budget.of(proto);

        assertThat(model.getName(), is("billingAccounts/0123AB-4567CD-89EF01/budgets/my-budget"));
        assertThat(model.getDisplayName(), is("Monthly Analytics Budget"));
        assertThat(model.getSpecifiedAmount(), closeTo(250.50, 0.001));
        assertThat(model.getCurrencyCode(), is("USD"));
        assertThat(model.getThresholdPercentRules(), hasSize(3));
        assertThat(model.getThresholdPercentRules(), contains(0.5, 0.9, 1.0));
    }
}
