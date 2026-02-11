package io.kestra.plugin.gcp.gcs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "Post-processing action",
        description = "MOVE (copy then delete), DELETE, or NONE (caller handles cleanup to avoid retriggers)"
    )
    @NotNull
    Property<Action> getAction();

    @Schema(
        title = "Move destination",
        description = "Required when action is MOVE; gs:// prefix"
    )
    Property<String> getMoveDirectory();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}
