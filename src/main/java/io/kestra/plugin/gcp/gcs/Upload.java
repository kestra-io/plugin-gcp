package io.kestra.plugin.gcp.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "from: \"{{ inputs.file }}\"",
                "to: \"gs://my_bucket/dir/file.csv\""
            }
        )
    }
)
@Schema(
    title = "Upload a file to a GCS bucket."
)
public class Upload extends AbstractGcs implements RunnableTask<Upload.Output> {
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));
        URI to = new URI(runContext.render(this.to));

        BlobInfo destination = BlobInfo
            .newBuilder(BlobId.of(to.getScheme().equals("gs") ? to.getAuthority() : to.getScheme(), to.getPath().substring(1)))
            .build();

        logger.debug("Upload from '{}' to '{}'", from, to);

        InputStream data = runContext.uriToInputStream(from);

        long size = 0;
        try (WriteChannel writer = connection.writer(destination)) {
            byte[] buffer = new byte[10_240];

            int limit;
            while ((limit = data.read(buffer)) >= 0) {
                writer.write(ByteBuffer.wrap(buffer, 0, limit));
                size += limit;
            }
        }

        runContext.metric(Counter.of("file.size", size));

        return Output
            .builder()
            .uri(new URI("gs://" + destination.getBucket() + "/" + destination.getName()))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private URI uri;
    }
}
