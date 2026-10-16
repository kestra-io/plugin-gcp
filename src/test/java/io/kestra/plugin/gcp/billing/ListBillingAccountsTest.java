package io.kestra.plugin.gcp.billing;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.gcp.billing.models.BillingAccount;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ListBillingAccountsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldBuildAndRenderTaskProperties() throws Exception {
        var task = ListBillingAccounts.builder()
            .id("list_billing_accounts")
            .type(ListBillingAccounts.class.getName())
            .projectId(Property.ofExpression("{{ project_var }}"))
            .build();

        var runContext = runContextFactory.of(Map.of(
            "project_var", "my-finops-project"
        ));

        var rProjectId = runContext.render(task.getProjectId()).as(String.class).orElseThrow();
        assertThat(rProjectId, is("my-finops-project"));
    }

    @Test
    void shouldMapBillingAccountFromProto() {
        var proto = com.google.cloud.billing.v1.BillingAccount.newBuilder()
            .setName("billingAccounts/0123AB-4567CD-89EF01")
            .setDisplayName("Production Billing")
            .setOpen(true)
            .setMasterBillingAccount("billingAccounts/MASTER-123")
            .build();

        var model = BillingAccount.of(proto);

        assertThat(model.getName(), is("billingAccounts/0123AB-4567CD-89EF01"));
        assertThat(model.getDisplayName(), is("Production Billing"));
        assertThat(model.isOpen(), is(true));
        assertThat(model.getMasterBillingAccount(), is("billingAccounts/MASTER-123"));
    }
}
