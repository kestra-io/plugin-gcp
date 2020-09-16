package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
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
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Delete a bucket."
)
@Example(
    title = "Delete a bucket",
    code = {
        "name: \"my-bucket\""
    }
)
public class DeleteBucket extends Task implements RunnableTask<DeleteBucket.Output> {
    @NotNull
    @InputProperty(
        description = "Bucket's unique name",
        dynamic = true
    )
    protected String name;

    @InputProperty(
        description = "The GCP project id",
        dynamic = true
    )
    protected String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger();
        String name = runContext.render(this.name);

        logger.debug("Deleting bucket '{}'", name);

        boolean delete = connection.delete(name);

        if (!delete) {
            throw new StorageException(404, "Couldn't find bucket '" + name + "'");
        }

        return Output
            .builder()
            .bucket(name)
            .bucketUri(new URI("gs://" + name))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The bucket's unique name"
        )
        private String bucket;

        @OutputProperty(
            description = "The bucket's URI"
        )
        private URI bucketUri;
    }
}
