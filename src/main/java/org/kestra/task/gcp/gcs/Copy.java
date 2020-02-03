package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
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
@Documentation(
    description = "Copy a file between bucket",
    body = "Copy the file between Internal Storage or Google Cloud Storage file"
)
public class Copy extends Task implements RunnableTask<Copy.Output> {
    @InputProperty(
        description = "The file to copy",
        dynamic = true
    )
    private String from;

    @InputProperty(
        description = "The destination path",
        dynamic = true
    )
    private String to;

    @InputProperty(
        description = "The google project id to use",
        dynamic = true
    )
    private String projectId;

    @InputProperty(
        description = "Delete the from files on success copy"
    )
    @Builder.Default
    private boolean delete = false;

    @Override
    public Copy.Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        URI from = new URI(runContext.render(this.from));
        URI to = new URI(runContext.render(this.to));

        BlobId source = BlobId.of(from.getScheme().equals("gs") ? from.getAuthority() : from.getScheme(), from.getPath().substring(1));

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
        @OutputProperty(
            description = "The destination full uri",
            body = {"The full url will be in like `gs://{bucket}/{path}/{file}`"}
        )
        private URI uri;
    }
}
