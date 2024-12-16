package io.kestra.plugin.gcp.gcs.models;

import com.google.cloud.storage.Acl;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
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
    private final Property<Role> role;

    public enum Role {
        READER,
        WRITER,
        OWNER
    }

    public static List<Acl> convert(List<AccessControl> accessControls, RunContext runContext) {
        return accessControls
            .stream()
            .map(c -> {
                try {
                    return c.convert(runContext);
                } catch (IllegalVariableEvaluationException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
    }

    private Acl convert(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.getEntity() == null || this.getRole() == null) {
            return null;
        }

        var role = runContext.render(this.getRole()).as(Role.class);
        var value = runContext.render(this.getEntity().getValue()).as(String.class);

        switch (runContext.render(this.getEntity().getType()).as(Entity.Type.class).orElseThrow()) {
            case USER:
                return Acl.of(new Acl.User(value.get()), Acl.Role.valueOf(role.get().name()));
            case GROUP:
                return Acl.of(new Acl.Group(value.get()), Acl.Role.valueOf(role.get().name()));
            case DOMAIN:
                return Acl.of(new Acl.Domain(value.get()), Acl.Role.valueOf(role.get().name()));
            default:
                return null;
        }
    }
}
