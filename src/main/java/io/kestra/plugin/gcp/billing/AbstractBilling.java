package io.kestra.plugin.gcp.billing;

import java.io.IOException;
import java.util.List;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.billing.budgets.v1.BudgetServiceClient;
import com.google.cloud.billing.budgets.v1.BudgetServiceSettings;
import com.google.cloud.billing.v1.CloudBillingClient;
import com.google.cloud.billing.v1.CloudBillingSettings;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.CredentialService;
import io.kestra.plugin.gcp.shared.GcpInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBilling extends Task implements GcpInterface {
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

    protected CloudBillingClient cloudBillingClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        var credentials = CredentialService.connection(runContext, this).credentials();
        var settingsBuilder = CloudBillingSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        return CloudBillingClient.create(settingsBuilder.build());
    }

    protected BudgetServiceClient budgetServiceClient(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        var credentials = CredentialService.connection(runContext, this).credentials();
        var settingsBuilder = BudgetServiceSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        return BudgetServiceClient.create(settingsBuilder.build());
    }
}
