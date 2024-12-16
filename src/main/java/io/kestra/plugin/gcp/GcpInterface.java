package io.kestra.plugin.gcp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

import java.util.List;

public interface GcpInterface {
    @Schema(title = "The GCP project ID.")
    Property<String> getProjectId();

    @Schema(title = "The GCP service account.")
    Property<String> getServiceAccount();

    @Schema(title = "The GCP service account to impersonate.")
    Property<String> getImpersonatedServiceAccount();

    @Schema(title = "The GCP scopes to be used.")
    Property<List<String>> getScopes();
}