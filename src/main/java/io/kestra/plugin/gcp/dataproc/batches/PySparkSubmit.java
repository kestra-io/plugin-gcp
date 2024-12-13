package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.PySparkBatch;
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
    title = "Submit an [Apache PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/) batch workload."
)
@Plugin(
    examples = @Example(
        full = true,
        code = """
            id: gcp_dataproc_py_spark_submit
            namespace: company.team
            tasks:
              - id: py_spark_submit
                type: io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit
                mainPythonFileUri: 'gs://spark-jobs-kestra/pi.py'
                name: test-pyspark
                region: europe-west3
            """
    )
)
public class PySparkSubmit extends AbstractSparkSubmit {
    //Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix
    @Schema(
        title = "The HCFS URI of the main Python file to use as the Spark driver. Must be a .py file.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
    )
    @NotNull
    protected Property<String> mainPythonFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        PySparkBatch.Builder sparkBuilder = PySparkBatch.newBuilder();

        sparkBuilder.setMainPythonFileUri(runContext.render(mainPythonFileUri).as(String.class).orElseThrow());

        var renderedJarFileUris = runContext.render(this.jarFileUris).asList(String.class);
        if (!renderedJarFileUris.isEmpty()) {
            sparkBuilder.addAllJarFileUris(renderedJarFileUris);
        }

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

        builder.setPysparkBatch(sparkBuilder.build());
    }
}
