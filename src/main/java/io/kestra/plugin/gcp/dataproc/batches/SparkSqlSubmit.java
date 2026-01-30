package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkSqlBatch;
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
    title = "Submit Spark SQL batch to Dataproc",
    description = "Executes Spark SQL from a script file; supports extra JARs for UDFs."
)
@Plugin(
    examples = @Example(
        full = true,
        code = """
            id: gcp_dataproc_spark_sql_submit
            namespace: company.team
            tasks:
              - id: spark_sql_submit
                type: io.kestra.plugin.gcp.dataproc.batches.SparkSqlSubmit
                queryFileUri: 'gs://spark-jobs-kestra/foobar.py'
                name: test-sparksql
                region: europe-west3
            """
    )
)
public class SparkSqlSubmit extends AbstractSparkSubmit {
    @Schema(
        title = "Query file URI",
        description = "HCFS URI to the Spark SQL script (gs://, hdfs://, or file://)"
    )
    @NotNull
    protected Property<String> queryFileUri;

    @Override
    protected void buildBatch(Batch.Builder builder, RunContext runContext) throws IllegalVariableEvaluationException {
        SparkSqlBatch.Builder sparkBuilder = SparkSqlBatch.newBuilder();

        sparkBuilder.setQueryFileUri(runContext.render(queryFileUri).as(String.class).orElseThrow());

        var renderedJarFileUris = runContext.render(this.jarFileUris).asList(String.class);
        if (!renderedJarFileUris.isEmpty()) {
            sparkBuilder.addAllJarFileUris(renderedJarFileUris);
        }

        builder.setSparkSqlBatch(sparkBuilder.build());
    }
}
