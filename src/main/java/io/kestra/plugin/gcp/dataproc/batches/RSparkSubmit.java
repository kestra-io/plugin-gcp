package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkRBatch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit an [Apache SparkR](https://spark.apache.org/docs/latest/sparkr.html) batch workload."
)
@Plugin(
    examples = @Example(
        code = {
            "mainRFileUri: 'gs://spark-jobs-kestra/dataframe.r'",
            "name: test-rspark"
        }
    )
)
public class RSparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The HCFS URI of the main R file to use as the driver. Must be a `.R` or `.r` file.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String mainRFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkRBatch.Builder sparkBuilder = SparkRBatch.newBuilder();

        sparkBuilder.setMainRFileUri(runContext.render(mainRFileUri));

        if (this.fileUris != null) {
            sparkBuilder.addAllFileUris(runContext.render(this.fileUris));
        }

        if (this.archiveUris != null) {
            sparkBuilder.addAllArchiveUris(runContext.render(this.archiveUris));
        }

        if (this.args != null) {
            sparkBuilder.addAllArgs(runContext.render(this.args));
        }

        builder.setSparkRBatch(sparkBuilder.build());
    }
}
