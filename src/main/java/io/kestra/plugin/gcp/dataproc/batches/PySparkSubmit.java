package io.kestra.plugin.gcp.dataproc.batches;

import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.PySparkBatch;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Submit a PySpark batch to Dataproc",
    description = "Runs a PySpark batch from a main Python file; supports extra JARs, files, archives, and args."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Submit a PySpark batch to Dataproc",
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
        ),
        @Example(
            full = true,
            title = "Upload a PySpark script to Cloud Storage, then run it as a Dataproc batch",
            code = """
                id: gcp_dataproc_py_spark_upload_and_run
                namespace: company.team

                inputs:
                  - id: script
                    type: FILE

                tasks:
                  - id: upload_script
                    type: io.kestra.plugin.gcp.gcs.Upload
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    from: "{{ inputs.script }}"
                    to: "gs://my-bucket/jobs/{{ execution.id }}/job.py"

                  - id: py_spark_submit
                    type: io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    region: europe-west3
                    name: pyspark-job
                    mainPythonFileUri: "{{ outputs.upload_script.uri }}"
                """
        ),
        @Example(
            full = true,
            title = "Read from and write to BigQuery from a PySpark batch (no connector JAR needed)",
            code = """
                id: gcp_dataproc_py_spark_bigquery
                namespace: company.team
                tasks:
                  - id: py_spark_submit
                    type: io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    region: europe-west3
                    name: pyspark-bigquery
                    mainPythonFileUri: 'gs://my-bucket/jobs/bigquery_etl.py'
                    args:
                      - "--source-table"
                      - "my_dataset.orders"
                      - "--destination-table"
                      - "my_dataset.orders_summary"
                      - "--temporary-gcs-bucket"
                      - "my-bucket-tmp"
                """
        ),
        @Example(
            full = true,
            title = "Submit a PySpark batch with extra JARs, files, archives and driver arguments",
            code = """
                id: gcp_dataproc_py_spark_dependencies
                namespace: company.team
                tasks:
                  - id: py_spark_submit
                    type: io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    region: europe-west3
                    name: pyspark-with-dependencies
                    mainPythonFileUri: 'gs://my-bucket/jobs/etl_job.py'
                    jarFileUris:
                      - 'gs://my-bucket/libs/postgresql-42.7.3.jar'
                    fileUris:
                      - 'gs://my-bucket/config/settings.json'
                    archiveUris:
                      - 'gs://my-bucket/data/lookup-tables.zip'
                    args:
                      - "--input"
                      - "gs://my-bucket/data/input/"
                      - "--output"
                      - "gs://my-bucket/data/output/"
                """
        ),
        @Example(
            full = true,
            title = "Run a PySpark batch on a custom container image from Artifact Registry",
            code = """
                id: gcp_dataproc_py_spark_container
                namespace: company.team
                tasks:
                  - id: py_spark_submit
                    type: io.kestra.plugin.gcp.dataproc.batches.PySparkSubmit
                    projectId: "{{ secret('GCP_PROJECT_ID') }}"
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_KEY') }}"
                    region: europe-west3
                    name: pyspark-custom-image
                    mainPythonFileUri: 'gs://my-bucket/jobs/ml_job.py'
                    runtime:
                      containerImage: "europe-west3-docker.pkg.dev/{{ secret('GCP_PROJECT_ID') }}/spark/pyspark-ml:1.0.0"
                """
        )
    }
)
public class PySparkSubmit extends AbstractSparkSubmit {
    //Can be a GCS file with the gs:// prefix, an HDFS file on the cluster with the hdfs:// prefix, or a local file on the cluster with the file:// prefix
    @Schema(
        title = "Main Python file URI",
        description = "HCFS URI to the driver .py file (gs://, hdfs://, or file://)"
    )
    @NotNull
    @PluginProperty(group = "main")
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
