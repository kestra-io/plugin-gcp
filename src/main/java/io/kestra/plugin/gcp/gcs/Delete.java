package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
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
            code = {
                "uri: \"gs://my_bucket/dir/file.csv\""
            }
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
    @PluginProperty(dynamic = true)
    private String uri;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @PluginProperty
    @Builder.Default
    private final Boolean errorOnMissing = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI render = encode(runContext, this.uri);

        BlobId source = BlobId.of(
            render.getAuthority(),
            blobPath(render.getPath().substring(1))
        );

        logger.debug("Delete '{}'", render);

        boolean delete = connection.delete(source);

        if (errorOnMissing && !delete) {
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
