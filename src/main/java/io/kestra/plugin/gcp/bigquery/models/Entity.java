package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import javax.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class Entity {
    @NotNull
    @Schema(
        title = "The type of the entity (USER, GROUP, DOMAIN or IAM_MEMBER)."
    )
    @PluginProperty(dynamic = true)
    private final Type type;

    @NotNull
    @Schema(
        title = "The value for the entity.",
        description = "For example, user email if the type is USER."
    )
    @PluginProperty(dynamic = true)
    private final String value;

    @SuppressWarnings("unused")
    public enum Type {
        DOMAIN,
        GROUP,
        USER,
        IAM_MEMBER
    }
}
