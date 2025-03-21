package io.kestra.plugin.gcp.bigquery.models;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class AccessControl {
    @NotNull
    @Schema(
        title = "The GCP entity."
    )
    private final Entity entity;

    @NotNull
    @Schema(
        title = "The role to assign to the entity."
    )
    private final Property<Role> role;

    public enum Role {
        READER,
        WRITER,
        OWNER
    }
}
