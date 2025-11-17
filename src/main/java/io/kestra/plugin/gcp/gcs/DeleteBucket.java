package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a GCS bucket."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a bucket",
            full = true,
            code = """
                id: gcp_gcs_delete_bucket
                namespace: company.team

                tasks:
                  - id: delete_bucket
                    type: io.kestra.plugin.gcp.gcs.DeleteBucket
                    name: "my-bucket"
                """
        )
    }
)
public class DeleteBucket extends AbstractGcs implements RunnableTask<DeleteBucket.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    private Property<String> name;

    @Schema(
        title = "If set to true, any blobs in the bucket will be deleted prior to bucket deletion. The default is false"
    )
    @Builder.Default
    private Property<Boolean> force = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        String name = runContext.render(this.name).as(String.class).orElseThrow();
        boolean rForce = runContext.render(this.force).as(Boolean.class).orElse(false);

        if (rForce) {
            DeleteList deleteListTask = DeleteList.builder()
                .id(this.id)
                .type(DeleteList.class.getName())
                .projectId(this.projectId)
                .serviceAccount(this.serviceAccount)
                .scopes(this.scopes)
                .from(Property.ofValue("gs://" + name))
                .concurrent(8)
                .build();

            deleteListTask.run(runContext);
        }

        logger.debug("Deleting bucket '{}'", name);

        boolean delete = connection.delete(name);

        if (!delete) {
            throw new StorageException(404, "Couldn't find bucket '" + name + "'");
        }

        return Output
            .builder()
            .bucket(name)
            .bucketUri(new URI("gs://" + name))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket's unique name"
        )
        private String bucket;

        @Schema(
            title = "The bucket's URI"
        )
        private URI bucketUri;
    }
}
