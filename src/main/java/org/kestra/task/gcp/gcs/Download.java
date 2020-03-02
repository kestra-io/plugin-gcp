package org.kestra.task.gcp.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    code = {
        "from: \"gs://my_bucket/dir/file.csv\""
    }
)
@Documentation(
    description = "Download a file to a GCS bucket."
)
public class Download extends Task implements RunnableTask<Download.Output> {
    @InputProperty(
        description = "The file to copy",
        dynamic = true
    )
    private String from;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    private String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));

        Logger logger = runContext.logger(this.getClass());
        URI from = new URI(runContext.render(this.from));

        BlobId source = BlobId.of(
            from.getAuthority(),
            from.getPath().substring(1)
        );

        ReadChannel readChannel = connection.get(source).reader();

        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        FileOutputStream fileOuputStream = new FileOutputStream(tempFile);
        fileOuputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
        fileOuputStream.close();

        logger.debug("Download from '{}'", from);

        return Output
            .builder()
            .bucket(source.getBucket())
            .path(source.getName())
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The bucket of the downloaded file"
        )
        private final String bucket;

        @OutputProperty(
            description = "The path on the bucket of the downloaded file"
        )
        private final String path;

        @OutputProperty(
            description = "The url of the downloaded file on kestra storage "
        )
        private URI uri;
    }
}
