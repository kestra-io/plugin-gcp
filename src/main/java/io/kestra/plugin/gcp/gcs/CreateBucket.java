package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.gcs.models.Bucket;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Create a new bucket with some options",
            full = true,
            code = """
                id: gcp_gcs_create_bucket
                namespace: company.name

                tasks:
                  - id: create_bucket
                    type: io.kestra.plugin.gcp.gcs.CreateBucket
                    name: "my-bucket"
                    versioningEnabled: true
                    labels: 
                      my-label: my-value
                """
        )
    }
)
@Schema(
    title = "Create a bucket or update if it already exists."
)
public class CreateBucket extends AbstractBucket implements RunnableTask<AbstractBucket.Output> {
    @Builder.Default
    @Schema(
        title = "Policy to apply if a bucket already exists."
    )
    private IfExists ifExists = IfExists.ERROR;

    @Override
    public AbstractBucket.Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        BucketInfo bucketInfo = this.bucketInfo(runContext);

        com.google.cloud.storage.Bucket bucket = connection.get(bucketInfo.getName());

        // Bucket does not exist, we try to create it
        if (bucket == null) {
            logger.debug("Creating bucket '{}'", bucketInfo);
            return Output.builder()
                .bucket(Bucket.of(connection.create(bucketInfo)))
                .created(true)
                .build();
        }

        // Bucket exists, we check the ifExists policy
        if (this.ifExists == IfExists.UPDATE) {
            logger.debug("Updating bucket '{}'", bucketInfo);
            return Output.builder()
                .bucket(Bucket.of(connection.update(bucketInfo)))
                .updated(true)
                .build();

        } else if (this.ifExists == IfExists.SKIP) {
            logger.debug("Bucket '{}' already exists, skipping", bucketInfo);
            return Output.builder()
                .bucket(Bucket.of(connection.update(bucketInfo)))
                .build();
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
