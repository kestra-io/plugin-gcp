package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;
import java.util.NoSuchElementException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: gcp_gcs_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.gcp.gcs.Delete
                    uri: "gs://my_bucket/dir/file.csv"
                """
        )
    }
)
@Schema(
    title = "Delete a file to a GCS bucket."
)
public class Delete extends AbstractGcs implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The file to delete"
    )
    private Property<String> uri;

    @Schema(
        title = "Raise an error if the file is not found"
    )
    @Builder.Default
    private final Property<Boolean> errorOnMissing = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI render = encode(runContext, runContext.render(this.uri).as(String.class).orElse(null));

        BlobId source = BlobId.of(
            render.getAuthority(),
            blobPath(render.getPath().substring(1))
        );

        logger.debug("Delete '{}'", render);

        boolean delete = connection.delete(source);

        if (runContext.render(errorOnMissing).as(Boolean.class).orElse(false) && !delete) {
            throw new NoSuchElementException("Unable to find file '" + render + "'");
        }

        return Output
            .builder()
            .uri(render)
            .deleted(delete)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The deleted uri"
        )
        private final URI uri;

        @Schema(
            title = "If the files was really deleted"
        )
        private final Boolean deleted;
    }
}
