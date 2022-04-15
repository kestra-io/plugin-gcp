package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.PySparkBatch;
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
    title = "Submit an [Apache PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html) batch workload."
)
public class PySparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The URI of the main Python file to use as the Spark driver.",
        description = "Must be a .py file."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String mainPythonFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        PySparkBatch.Builder sparkBuilder = PySparkBatch.newBuilder();

        sparkBuilder.setMainPythonFileUri(runContext.render(mainPythonFileUri));

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

        builder.setPysparkBatch(sparkBuilder.build());
    }
}
