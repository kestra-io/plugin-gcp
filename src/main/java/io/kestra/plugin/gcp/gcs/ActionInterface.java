package io.kestra.plugin.gcp.gcs;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    ActionInterface.Action getAction();

    @Schema(
        title = "The destination directory for `MOVE` action."
    )
    @PluginProperty(dynamic = true)
    String getMoveDirectory();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}
