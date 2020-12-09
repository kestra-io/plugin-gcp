package org.kestra.task.gcp;

import io.swagger.v3.oas.annotations.media.Schema;
import org.kestra.core.models.annotations.PluginProperty;

import java.util.List;

public interface GcpInterface {
    @Schema(
        title = "The GCP project id"
    )
    @PluginProperty(dynamic = true)
    String getProjectId();

    @Schema(
        title = "The GCP service account key"
    )
    @PluginProperty(dynamic = true)
    String getServiceAccount();

    @Schema(
        title = "The GCP scopes to used"
    )
    @PluginProperty(dynamic = true)
    List<String> getScopes();
}
