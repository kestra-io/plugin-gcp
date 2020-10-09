package org.kestra.task.gcp.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
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
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;

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

    static File download(Storage connection, BlobId source) throws IOException {
        Blob blob = connection.get(source);
        if (blob == null) {
            throw new IllegalArgumentException("Unable to find blob on bucket '" +  source.getBucket() +"' with path '" +  source.getName() +"'");
        }
        ReadChannel readChannel = blob.reader();

        File tempFile = File.createTempFile(
            Download.class.getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(source.getName())
        );

        try (
            FileOutputStream fileOuputStream = new FileOutputStream(tempFile);
            FileChannel channel = fileOuputStream.getChannel()
        ) {
            channel.transferFrom(readChannel, 0, Long.MAX_VALUE);
        }

        return tempFile;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));

        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        BlobId source = BlobId.of(
            from.getAuthority(),
            from.getPath().substring(1)
        );

        File tempFile = download(connection, source);
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
        private final URI uri;
    }
}
