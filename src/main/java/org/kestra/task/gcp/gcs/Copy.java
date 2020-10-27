package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy a file between bucket",
    description = "Copy the file between Internal Storage or Google Cloud Storage file"
)
@Plugin(
    examples = {
        @Example(
            title = "Move a file between bucket path",
            code = {
                "from: \"{{ inputs.file }}\"",
                "delete: true"
            }
        )
    }
)
public class Copy extends Task implements RunnableTask<Copy.Output> {
    @Schema(
        title = "The file to copy"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The destination path"
    )
    @PluginProperty(dynamic = true)
    private String to;

    @Schema(
        title = "The GCP project id"
    )
    @PluginProperty(dynamic = true)
    private String projectId;

    @Schema(
        title = "Whether to delete the source files (from parameter) on success copy"
    )
    @Builder.Default
    private final boolean delete = false;

    @Override
    public Copy.Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));
        URI to = new URI(runContext.render(this.to));

        BlobId source = BlobId.of(from.getScheme().equals("gs") ? from.getAuthority() : from.getScheme(), from.getPath().substring(1));

        if (from.toString().equals(to.toString())) {
            throw new IllegalArgumentException("Invalid copy to same path '" + to.toString());
        }

        logger.debug("Moving from '{}' to '{}'", from, to);

        Blob result = connection
            .copy(Storage.CopyRequest.newBuilder()
                .setSource(source)
                .setTarget(BlobId.of(to.getAuthority(), to.getPath().substring(1)))
                .build()
            )
            .getResult();

        runContext.metric(Counter.of("file.size", result.getSize()));

        if (this.delete) {
            connection.delete(source);
        }

        return Output
            .builder()
            .uri(new URI("gs://" + result.getBucket() + "/" + result.getName()))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The destination full uri",
            description = "The full url will be like `gs://{bucket}/{path}/{file}`"
        )
        private URI uri;
    }
}
