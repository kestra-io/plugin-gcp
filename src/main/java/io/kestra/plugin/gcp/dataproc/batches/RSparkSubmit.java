package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkRBatch;
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
    title = "Submit an [Apache SparkR](https://spark.apache.org/docs/latest/sparkr.html) batch workload."
)
public class RSparkSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The URI of the main R file to use as the driver.",
        description = "Must be a `.R` or `.r` file."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String mainPythonFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkRBatch.Builder sparkBuilder = SparkRBatch.newBuilder();

        sparkBuilder.setMainRFileUri(runContext.render(mainPythonFileUri));

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
