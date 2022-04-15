package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkBatch;
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

import java.util.List;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSparkSubmit extends AbstractBatch {
    @Schema(
        title = "URIs of files to be placed in the working directory of each executor."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> fileUris;

    @Schema(
        title = "URIs of archives to be extracted into the working director of each executor.",
        description = "Supported file types: `.jar`, `.tar`, `.tar.gz`, `.tgz`, and `.zip`."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> archiveUris;

    @Schema(
        title = "URIs of jar files to add to the classpath of the Spark driver and tasks."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> jarFileUris;

    @Schema(
        title = "The arguments to pass to the driver.",
        description = "Do not include arguments that can be set as batch properties, such as `--conf`, since a collision " +
            "can occur that causes an incorrect batch submission."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected List<String> args;
}
