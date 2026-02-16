package io.kestra.plugin.gcp.gcs;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
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
            title = "Update some bucket labels",
            full = true,
            code = """
                id: gcp_gcs_update_bucket
                namespace: company.team

                tasks:
                  - id: update_bucket
                    type: io.kestra.plugin.gcp.gcs.UpdateBucket
                    name: "my-bucket"
                    labels:
                      my-label: my-value
                """
        )
    }
)
@Schema(
    title = "Update a GCS bucket",
    description = "Updates bucket metadata such as labels, lifecycle, IAM config, logging, and defaults."
)
public class UpdateBucket extends AbstractBucket implements RunnableTask<AbstractBucket.Output> {
    @Override
    public AbstractBucket.Output run(RunContext runContext) throws Exception {
        Storage connection = this.connection(runContext);

        Logger logger = runContext.logger();
        BucketInfo bucketInfo = this.bucketInfo(runContext);

        logger.debug("Updating bucket '{}'", bucketInfo);

        return Output.builder()
            .bucket(io.kestra.plugin.gcp.gcs.models.Bucket.of(connection.update(bucketInfo)))
            .updated(true)
            .build();

    }
}
