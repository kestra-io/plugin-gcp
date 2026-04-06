package io.kestra.plugin.gcp.gcs.models;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import io.kestra.core.models.annotations.PluginProperty;

@Getter
@Builder
@Jacksonized
public class Entity {
    @NotNull
    @Schema(
        title = "The type of the entity (USER, GROUP or DOMAIN)"
    )
    @PluginProperty(group = "main")
    private final Property<Type> type;

    @NotNull
    @Schema(
        title = "The value for the entity (ex : user email if the type is USER ...)"
    )
    @PluginProperty(group = "main")
    private final Property<String> value;

    public enum Type {
        DOMAIN,
        GROUP,
        USER
    }
}
