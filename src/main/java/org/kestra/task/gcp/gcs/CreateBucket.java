package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

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
@Documentation(
    description = "Create a bucket or update if it already exists."
)
public class CreateBucket extends AbstractBucket implements RunnableTask<AbstractBucket.Output> {
    @Builder.Default
    @InputProperty(
        description = "Policy to apply if a bucket already exists."
    )
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

    private Bucket create(Storage connection, RunContext runContext, BucketInfo bucketInfo) throws IllegalVariableEvaluationException {
        Bucket bucket = connection.get(bucketInfo.getName());

        // Bucket does not exist, we try to create it
        if (bucket == null) {
            return connection.create(bucketInfo);
        }

        // Bucket exists, we check the ifExists policy
        if (this.ifExists == IfExists.UPDATE) {
            return connection.update(bucketInfo);
        } else if (this.ifExists == IfExists.SKIP) {
            return bucket;
        } else {
            throw new RuntimeException("Bucket " + bucketInfo.getName() + " already exists and ifExists policy is set to ERROR !");
        }
    }

    public enum IfExists {
        ERROR,
        UPDATE,
        SKIP
    }
}
