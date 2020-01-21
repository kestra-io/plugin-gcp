package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunOutput;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class UpdateBucket extends AbstractBucket implements RunnableTask {
    @Override
    public RunOutput run(RunContext runContext) throws Exception {
        Storage connection = new Connection().of(runContext.render(this.projectId));
        Logger logger = runContext.logger(this.getClass());
        BucketInfo bucketInfo = this.bucketInfo(runContext);

        logger.debug("Updating bucket '{}'", bucketInfo);
        Bucket bucket = connection.update(bucketInfo);

        return RunOutput
            .builder()
            .outputs(this.outputs(bucket))
            .build();
    }
}
