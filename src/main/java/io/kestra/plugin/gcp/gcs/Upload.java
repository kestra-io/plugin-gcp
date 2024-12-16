package io.kestra.plugin.gcp.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
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
            full = true,
            code = """
                id: gcp_gcs_upload
                namespace: company.team

                tasks:
                  - id: upload
                    type: io.kestra.plugin.gcp.gcs.Upload
                    from: "{{ inputs.file }}"
                    to: "gs://my_bucket/dir/file.csv"
                """
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
    private Property<String> from;

    @Schema(
        title = "The destination path"
    )
    private Property<String> to;

    @Schema(
        title = "The blob's data content type."
    )
    private Property<String> contentType;

    @Schema(
        title = "The blob's data content encoding."
    )
    private Property<String> contentEncoding;

    @Schema(
        title = "The blob's data content disposition."
    )
    private Property<String> contentDisposition;

    @Schema(
        title = "The blob's data cache control."
    )
    private Property<String> cacheControl;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        URI from = encode(runContext, runContext.render(this.from).as(String.class).orElse(null));
        URI to = encode(runContext, runContext.render(this.to).as(String.class).orElse(null));

        BlobInfo.Builder builder = BlobInfo
            .newBuilder(BlobId.of(
                to.getScheme().equals("gs") ? to.getAuthority() : to.getScheme(),
                blobPath(to.getPath().substring(1))
            ));

        if (this.contentType != null) {
            builder.setContentType(runContext.render(this.contentType).as(String.class).orElseThrow());
        }

        if (this.contentEncoding != null) {
            builder.setContentEncoding(runContext.render(this.contentEncoding).as(String.class).orElseThrow());
        }

        if (this.contentDisposition != null) {
            builder.setContentDisposition(runContext.render(this.contentDisposition).as(String.class).orElseThrow());
        }

        if (this.cacheControl != null) {
            builder.setCacheControl(runContext.render(this.cacheControl).as(String.class).orElseThrow());
        }

        BlobInfo destination = builder.build();

        logger.debug("Upload from '{}' to '{}'", from, to);

        try (InputStream data = runContext.storage().getFile(from)) {
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
                .uri(new URI("gs://" + destination.getBucket() + "/" + encode(destination.getName())))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private URI uri;
    }
}
