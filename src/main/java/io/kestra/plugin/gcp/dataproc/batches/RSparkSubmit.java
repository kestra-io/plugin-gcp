package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkRBatch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
        full = true,
        code = """
            id: gcp_dataproc_r_spark_submit
            namespace: company.team
            tasks:
              - id: r_spark_submit
                type: io.kestra.plugin.gcp.dataproc.batches.RSparkSubmit
                mainRFileUri: 'gs://spark-jobs-kestra/dataframe.r'
                name: test-rspark
                region: europe-west3
            """
    )
)
public class RSparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The HCFS URI of the main R file to use as the driver. Must be a `.R` or `.r` file.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    @NotNull
    protected Property<String> mainRFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkRBatch.Builder sparkBuilder = SparkRBatch.newBuilder();

        sparkBuilder.setMainRFileUri(runContext.render(mainRFileUri).as(String.class).orElseThrow());

        var renderedFileUris = runContext.render(this.fileUris).asList(String.class);
        if (!renderedFileUris.isEmpty()) {
            sparkBuilder.addAllFileUris(renderedFileUris);
        }

        var renderedArchiveUris = runContext.render(this.archiveUris).asList(String.class);
        if (!renderedArchiveUris.isEmpty()) {
            sparkBuilder.addAllArchiveUris(renderedArchiveUris);
        }

        var renderedArgs = runContext.render(this.args).asList(String.class);
        if (!renderedArgs.isEmpty()) {
            sparkBuilder.addAllArgs(renderedArgs);
        }

        builder.setSparkRBatch(sparkBuilder.build());
    }
}
