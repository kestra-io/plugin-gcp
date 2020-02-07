package org.kestra.task.gcp.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    code = {
        "from: \"{{ inputs.file.uri }}\"",
        "to: \"gs://my_bucket/dir/file.csv\""
    }
)
public class Upload extends Task implements RunnableTask<Upload.Output> {
    private String from;
    private String to;
    private String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));

        Logger logger = runContext.logger(this.getClass());
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
    public static class Output implements org.kestra.core.models.tasks.Output {
        private URI uri;
    }
}
