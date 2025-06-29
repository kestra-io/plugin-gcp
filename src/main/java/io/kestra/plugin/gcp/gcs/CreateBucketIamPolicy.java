package io.kestra.plugin.gcp.gcs;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Add role to a service account on a bucket",
            full = true,
            code = """
                id: gcp_gcs_create_bucket_iam_policy
                namespace: company.team

                tasks:
                  - id: create_bucket_iam_policy
                    type: io.kestra.plugin.gcp.gcs.CreateBucketIamPolicy
                    name: "my-bucket"
                    member: "sa@project.iam.gserviceaccount.com"
                    role: "roles/storage.admin"
                """
        )
    }
)
@Schema(
    title = "Add role on an existing GCS bucket."
)
public class CreateBucketIamPolicy extends AbstractGcs implements RunnableTask<CreateBucketIamPolicy.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    protected Property<String> name;

    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    protected Property<String> member;

    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    protected Property<String> role;

    @Builder.Default
    @Schema(
        title = "Policy to apply if a policy already exists."
    )
    private Property<IfExists> ifExists = Property.ofValue(IfExists.SKIP);

    @Override
    public CreateBucketIamPolicy.Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();

        String bucketName = runContext.render(this.name).as(String.class).orElseThrow();
        Identity identity = Identity.valueOf(runContext.render(this.member).as(String.class).orElseThrow());
        Role role = Role.of(runContext.render(this.role).as(String.class).orElseThrow());

        Policy currentPolicy = connection.getIamPolicy(bucketName);

        boolean exists = currentPolicy
            .getBindingsList()
            .stream()
            .anyMatch(binding -> binding.getRole().equals(role.getValue()) &&
                binding
                    .getMembers()
                    .stream()
                    .anyMatch(s -> s.equals(identity.strValue()))
            );

        Output.OutputBuilder output = Output.builder()
            .bucket(bucketName)
            .member(identity.getValue())
            .role(role.getValue());

        if (exists) {
            String exception = "Binding " + role.getValue() + " for member " + member + " already exists";
            if (IfExists.ERROR.equals(runContext.render(this.ifExists).as(IfExists.class).orElseThrow())) {
                throw new Exception(exception);
            } else {
                logger.info(exception);
                return output.created(false).build();
            }
        }

        Policy updated = currentPolicy
            .toBuilder()
            .addIdentity(role, identity)
            .build();

        logger.debug("Updating policy on bucket '{}', adding '{}' with role '{}", bucketName, identity, role);

        connection.setIamPolicy(bucketName, updated);

        return output.created(true).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket uri"
        )
        private String bucket;

        @Schema(
            title = "The bucket uri"
        )
        private String member;

        @Schema(
            title = "The bucket uri"
        )
        private String role;

        @Schema(
            title = "If the binding was added, or already exist"
        )
        private Boolean created;
    }

    public enum IfExists {
        ERROR,
        SKIP
    }
}
