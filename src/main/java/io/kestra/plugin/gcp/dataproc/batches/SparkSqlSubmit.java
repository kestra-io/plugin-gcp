package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.SparkSqlBatch;
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
    title = "[Apache Spark SQL](https://spark.apache.org/sql/) queries as a batch workload."
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
        title = "The HCFS URI of the script that contains Spark SQL queries to execute.",
        description = "Hadoop Compatible File System (HCFS) URIs should be accessible from the cluster. Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix"
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
