package io.kestra.plugin.gcp;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

import java.util.List;

public interface GcpInterface {
    @Schema(title = "The GCP project ID.")
    @PluginProperty(dynamic = true)
    String getProjectId();

    @Schema(title = "The GCP service account.")
    @PluginProperty(dynamic = true)
    String getServiceAccount();

    @Schema(title = "The GCP service account to impersonate.")
    @PluginProperty(dynamic = true)
    String getImpersonatedServiceAccount();

    @Schema(title = "The GCP scopes to be used.")
    @PluginProperty(dynamic = true)
    List<String> getScopes();
}