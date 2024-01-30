package io.kestra.plugin.gcp.gcs.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class Entity {
    @NotNull
    @Schema(
        title = "The type of the entity (USER, GROUP or DOMAIN)"
    )
    @PluginProperty(dynamic = true)
    private final Type type;

    @NotNull
    @Schema(
        title = "The value for the entity (ex : user email if the type is USER ...)"
    )
    @PluginProperty(dynamic = true)
    private final String value;

    public enum Type {
        DOMAIN,
        GROUP,
        USER
    }
}
