package io.kestra.plugin.gcp.billing;

import java.io.IOException;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.billing.budgets.v1.BudgetServiceClient;
import com.google.cloud.billing.budgets.v1.BudgetServiceSettings;
import com.google.cloud.billing.v1.CloudBillingClient;
import com.google.cloud.billing.v1.CloudBillingSettings;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;

import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBilling extends AbstractTask {

    protected CloudBillingClient cloudBillingClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        var credentials = this.credentials(runContext);
        var settingsBuilder = CloudBillingSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        if (this.projectId != null) {
            runContext.render(this.projectId).as(String.class).ifPresent(settingsBuilder::setQuotaProjectId);
        }
        return CloudBillingClient.create(settingsBuilder.build());
    }

    protected BudgetServiceClient budgetServiceClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        var credentials = this.credentials(runContext);
        var settingsBuilder = BudgetServiceSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        if (this.projectId != null) {
            runContext.render(this.projectId).as(String.class).ifPresent(settingsBuilder::setQuotaProjectId);
        }
        return BudgetServiceClient.create(settingsBuilder.build());
    }

    protected IllegalStateException handleApiException(ApiException e, String operation, String requiredPermission) {
        return new IllegalStateException(
            "Google Cloud Billing API error during " + operation + " (Status: " + e.getStatusCode().getCode() + "). " +
            "Ensure the necessary API is enabled and caller has '" + requiredPermission + "' permission. Details: " + e.getMessage(),
            e
        );
    }
}
