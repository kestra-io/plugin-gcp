package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.Acl;
import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Jacksonized
public class AccessControl {
    @NotNull
    @Schema(
        title = "The entity"
    )
    @PluginProperty(dynamic = true)
    private final Entity entity;

    @NotNull
    @Schema(
        title = "The role to assign to the entity"
    )
    @PluginProperty(dynamic = true)
    private final Role role;

    public enum Role {
        READER,
        WRITER,
        OWNER
    }

    public static List<Acl> convert(List<AccessControl> accessControls) {
        return accessControls
            .stream()
            .map(c -> c.convert())
            .collect(Collectors.toList());
    }

    private Acl convert() {
        if (this.getEntity() == null || this.getRole() == null) {
            return null;
        }

        switch (this.getEntity().getType()) {
            case USER:
                return Acl.of(new Acl.User(this.getEntity().getValue()), Acl.Role.valueOf(this.getRole().name()));
            case GROUP:
                return Acl.of(new Acl.Group(this.getEntity().getValue()), Acl.Role.valueOf(this.getRole().name()));
            case DOMAIN:
                return Acl.of(new Acl.Domain(this.getEntity().getValue()), Acl.Role.valueOf(this.getRole().name()));
            default:
                return null;
        }
    }
}
