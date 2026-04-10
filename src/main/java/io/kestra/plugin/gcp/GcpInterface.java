package io.kestra.plugin.gcp;

import java.util.List;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface GcpInterface {
    @Schema(title = "The GCP project ID.")
    @PluginProperty(group = "connection")
    Property<String> getProjectId();

    @Schema(title = "The GCP service account.")
    @PluginProperty(group = "execution")
    Property<String> getServiceAccount();

    @Schema(title = "The GCP service account to impersonate.")
    @PluginProperty(group = "advanced")
    Property<String> getImpersonatedServiceAccount();

    @Schema(title = "The GCP scopes to be used.")
    @PluginProperty(group = "advanced")
    Property<List<String>> getScopes();
}