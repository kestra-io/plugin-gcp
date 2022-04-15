package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkSqlBatch;
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
    title = "[Apache Spark SQL](https://spark.apache.org/sql/) queries as a batch workload."
)
public class SparkSqlSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "The URI of the script that contains Spark SQL queries to execute."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String queryFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkSqlBatch.Builder sparkBuilder = SparkSqlBatch.newBuilder();

        sparkBuilder.setQueryFileUri(runContext.render(queryFileUri));

        if (this.jarFileUris != null) {
            sparkBuilder.addAllJarFileUris(runContext.render(this.jarFileUris));
        }

        builder.setSparkSqlBatch(sparkBuilder.build());
    }
}
