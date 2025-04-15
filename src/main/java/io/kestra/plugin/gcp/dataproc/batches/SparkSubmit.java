package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkBatch;
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
    title = "Submit an Apache Spark batch workload to a Google Cloud Dataproc cluster.",
    description = "For more details, check out the [Apache Spark documentation](https://spark.apache.org/)."
)
@Plugin(
    examples = @Example(
        full = true,
        code = """
            id: gcp_dataproc_spark_submit
            namespace: company.team
            tasks:
              - id: spark_submit
                type: io.kestra.plugin.gcp.dataproc.batches.SparkSubmit
                jarFileUris:
                  - 'gs://spark-jobs-kestra/spark-examples.jar'
                mainClass: org.apache.spark.examples.SparkPi
                args:
                  - 1000
                name: test-spark
                region: europe-west3
            """
    )
)
public class SparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The name of the driver main class.",
        description = "The jar file that contains the class must be in the classpath or specified in `jarFileUris`"
    )
    @NotNull
    private Property<String> mainClass;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkBatch.Builder sparkBuilder = SparkBatch.newBuilder();

        if (this.mainClass != null) {
            sparkBuilder.setMainClass(runContext.render(this.mainClass).as(String.class).orElseThrow());
        }

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

        builder.setSparkBatch(sparkBuilder.build());
    }
}
