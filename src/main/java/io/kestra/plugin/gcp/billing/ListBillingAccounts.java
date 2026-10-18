package io.kestra.plugin.gcp.billing;

import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.rpc.ApiException;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.billing.models.BillingAccount;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List GCP Billing Accounts",
    description = """
        Lists all Cloud Billing accounts that the authenticated service account has permission to view."""
)
@Plugin(
    examples = {
        @Example(
            title = "List all accessible GCP billing accounts",
            full = true,
            code = """
                id: gcp_list_billing_accounts
                namespace: company.finops

                tasks:
                  - id: list_accounts
                    type: io.kestra.plugin.gcp.billing.ListBillingAccounts
                    serviceAccount: "{{ secret('GCP_SA') }}"
                """
        )
    }
)
public class ListBillingAccounts extends AbstractBilling implements RunnableTask<ListBillingAccounts.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        logger.debug("Listing GCP billing accounts");

        var billingAccounts = new ArrayList<BillingAccount>();
        try (var client = this.cloudBillingClient(runContext)) {
            var response = client.listBillingAccounts();
            for (var account : response.iterateAll()) {
                billingAccounts.add(BillingAccount.of(account));
            }
        } catch (ApiException e) {
            throw handleApiException(e, "list billing accounts", "billing.accounts.get or billing.accounts.list");
        }

        logger.info("Retrieved {} billing accounts", billingAccounts.size());
        return Output.builder()
            .billingAccounts(billingAccounts)
            .count(billingAccounts.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The list of accessible billing accounts")
        private final List<BillingAccount> billingAccounts;

        @Schema(title = "The count of retrieved billing accounts")
        private final int count;
    }
}
