package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkBatch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit an [Apache Spark](https://spark.apache.org/) batch workload."
)
public class SparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The name of the driver main class.",
        description = "The jar file that contains the class must be in the classpath or specified in `jarFileUris`"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String mainClass;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkBatch.Builder sparkBuilder = SparkBatch.newBuilder();

        if (this.mainClass != null) {
            sparkBuilder.setMainClass(runContext.render(this.mainClass));
        }

        if (this.jarFileUris != null) {
            sparkBuilder.addAllJarFileUris(runContext.render(this.jarFileUris));
        }

        if (this.fileUris != null) {
            sparkBuilder.addAllFileUris(runContext.render(this.fileUris));
        }

        if (this.archiveUris != null) {
            sparkBuilder.addAllArchiveUris(runContext.render(this.archiveUris));
        }

        if (this.args != null) {
            sparkBuilder.addAllArgs(runContext.render(this.args));
        }

        builder.setSparkBatch(sparkBuilder.build());
    }
}
