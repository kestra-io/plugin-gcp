package io.kestra.plugin.gcp.billing.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class BillingAccount {
    @Schema(title = "The resource name of the billing account", description = "Format: billingAccounts/{billingAccountId}")
    String name;

    @Schema(title = "The display name of the billing account")
    String displayName;

    @Schema(title = "Whether the billing account is open and active")
    boolean open;

    @Schema(title = "The master billing account name, if this is a subaccount")
    String masterBillingAccount;

    public static BillingAccount of(com.google.cloud.billing.v1.BillingAccount account) {
        return BillingAccount.builder()
            .name(account.getName())
            .displayName(account.getDisplayName())
            .open(account.getOpen())
            .masterBillingAccount(account.getMasterBillingAccount())
            .build();
    }
}
