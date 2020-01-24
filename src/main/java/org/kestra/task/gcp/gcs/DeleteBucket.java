package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class DeleteBucket extends Task implements RunnableTask<DeleteBucket.Output> {
    @NotNull
    protected String name;
    protected String projectId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
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
        private String bucket;
        private URI bucketUri;
    }
}