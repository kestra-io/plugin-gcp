package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.slf4j.Logger;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CreateBucket extends AbstractBucket implements RunnableTask {
    @Builder.Default
    private IfExists ifExists = IfExists.ERROR;

    @Override
    public RunOutput run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        BucketInfo bucketInfo = this.bucketInfo(runContext);

        logger.debug("Creating bucket '{}'", bucketInfo);
        Bucket bucket = this.create(connection, runContext, bucketInfo);

        return RunOutput
            .builder()
            .outputs(this.outputs(bucket))
            .build();
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
