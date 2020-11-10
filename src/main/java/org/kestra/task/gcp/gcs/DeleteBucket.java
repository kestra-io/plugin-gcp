package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
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
@Schema(
    title = "Delete a bucket."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a bucket",
            code = {
                "name: \"my-bucket\""
            }
        )
    }
)
public class DeleteBucket extends AbstractGcs implements RunnableTask<DeleteBucket.Output> {
    @NotNull
    @Schema(
        title = "Bucket's unique name"
    )
    @PluginProperty(dynamic = true)
    protected String name;

    @Schema(
        title = "The GCP project id"
    )
    @PluginProperty(dynamic = true)
    protected String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

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
        @Schema(
            title = "The bucket's unique name"
        )
        private String bucket;

        @Schema(
            title = "The bucket's URI"
        )
        private URI bucketUri;
    }
}
