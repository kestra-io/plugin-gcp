package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;
import java.util.NoSuchElementException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    code = {
        "uri: \"gs://my_bucket/dir/file.csv\""
    }
)
@Documentation(
    description = "Delete a file to a GCS bucket."
)
public class Delete extends Task implements RunnableTask<Delete.Output> {
    @InputProperty(
        description = "The file to delete",
        dynamic = true
    )
    private String uri;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    private String projectId;

    @InputProperty(
        description = "raise an error if the file is not found",
        dynamic = false
    )
    @Builder.Default
    private Boolean errorOnMissing = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));

        Logger logger = runContext.logger();
        URI render = new URI(runContext.render(this.uri));

        BlobId source = BlobId.of(
            render.getAuthority(),
            render.getPath().substring(1)
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
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The deleted uri"
        )
        private final URI uri;

        @OutputProperty(
            description = "If the files was really deleted"
        )
        private final Boolean deleted;
    }
}
