package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Example(
    title = "Create a new bucket with some options",
    code = {
        "name: \"my-bucket\"",
        "versioningEnabled: true",
        "labels: ",
        "  my-label: my-value"
    }
)
public class CreateBucket extends AbstractBucket implements RunnableTask<AbstractBucket.Output> {
    @Builder.Default
    private IfExists ifExists = IfExists.ERROR;

    @Override
    public AbstractBucket.Output run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        BucketInfo bucketInfo = this.bucketInfo(runContext);

        logger.debug("Creating bucket '{}'", bucketInfo);
        Bucket bucket = this.create(connection, runContext, bucketInfo);

        return Output.of(bucket);
    }

    private Bucket create(Storage connection, RunContext runContext, BucketInfo bucketInfo ) throws IOException {
        Bucket bucket;
        try {
            bucket = connection.create(bucketInfo);
        } catch (StorageException exception) {
            boolean exists = exception.getCode() == 409;
            if (!exists) {
                throw exception;
            }

            if (this.ifExists == IfExists.UPDATE) {
                bucket = connection.update(bucketInfo);
            } else if (this.ifExists == IfExists.SKIP) {
                bucket = connection.get(runContext.render(this.name));
            } else {
                throw exception;
            }
        }

        return bucket;
    }

    public enum IfExists {
        ERROR,
        UPDATE,
        SKIP
    }
}
