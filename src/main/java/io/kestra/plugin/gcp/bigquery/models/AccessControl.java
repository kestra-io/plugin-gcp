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
public class AccessControl {
    @NotNull
    @Schema(
        title = "The GCP entity."
    )
    @PluginProperty(dynamic = true)
    private final Entity entity;

    @NotNull
    @Schema(
        title = "The role to assign to the entity."
    )
    @PluginProperty(dynamic = true)
    private final Role role;

    public enum Role {
        READER,
        WRITER,
        OWNER
    }
}
