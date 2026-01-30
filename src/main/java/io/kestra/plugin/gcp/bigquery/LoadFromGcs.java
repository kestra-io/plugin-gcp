package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Load an avro file from a gcs bucket",
            full = true,
            code = """
                id: gcp_bq_load_from_gcs
                namespace: company.team

                tasks:
                  - id: http_download
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/orders.csv

                  - id: csv_to_ion
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ outputs.http_download.uri }}"
                    header: true

                  - id: ion_to_avro
                    type: io.kestra.plugin.serdes.avro.IonToAvro
                    from: "{{ outputs.csv_to_ion.uri }}"
                    schema: |
                      {
                        "type": "record",
                        "name": "Order",
                        "namespace": "com.example.order",
                        "fields": [
                          {"name": "order_id", "type": "int"},
                          {"name": "customer_name", "type": "string"},
                          {"name": "customer_email", "type": "string"},
                          {"name": "product_id", "type": "int"},
                          {"name": "price", "type": "double"},
                          {"name": "quantity", "type": "int"},
                          {"name": "total", "type": "double"}
                        ]
                      }

                  - id: load_from_gcs
                    type: io.kestra.plugin.gcp.bigquery.LoadFromGcs
                    from:
                      - "{{ outputs.ion_to_avro.uri }}"
                    destinationTable: "my_project.my_dataset.my_table"
                    format: AVRO
                    avroOptions:
                      useAvroLogicalTypes: true
                """
        ),
        @Example(
            title = "Load a csv file with a defined schema",
            full = true,
            code = """
                id: gcp_bq_load_files_test
                namespace: company.team

                tasks:
                  - id: load_files_test
                    type: io.kestra.plugin.gcp.bigquery.LoadFromGcs
                    destinationTable: "myDataset.myTable"
                    ignoreUnknownValues: true
                    schema:
                      fields:
                        - name: colA
                          type: STRING
                        - name: colB
                          type: NUMERIC
                        - name: colC
                          type: STRING
                    format: CSV
                    csvOptions:
                      allowJaggedRows: true
                      encoding: UTF-8
                      fieldDelimiter: ","
                    from:
                      - gs://myBucket/myFile.csv
                """
        )
    },
    metrics = {
        @Metric(name = "bad.records", type = Counter.TYPE, unit = "records", description= "the number of bad records reported in a job."),
        @Metric(name = "duration", type = Timer.TYPE, description = "The time it took for the task to run."),
        @Metric(name = "input.bytes", type = Counter.TYPE, unit = "bytes", description = "The number of bytes of source data in a load job."),
        @Metric(name = "input.files", type = Counter.TYPE, unit = "files", description = "The number of source files in a load job."),
        @Metric(name = "output.bytes", type = Counter.TYPE, unit = "bytes", description = "The size of the data loaded by a load job so far, in bytes."),
        @Metric(name = "output.rows", type = Counter.TYPE, unit = "records", description = "The number of rows loaded by a load job so far.")
    }
)
@Schema(
    title = "Load GCS objects into BigQuery",
    description = "Runs a BigQuery load job from one or more GCS URIs into the destination table. Supports wildcard paths, format-specific options, and standard load limits. Table must exist unless schema is supplied with a write disposition that creates it."
)
public class LoadFromGcs extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Google Cloud Storage source data",
        description = "The fully-qualified URIs that point to source data in Google Cloud Storage (e.g." +
            " gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the" +
            " 'bucket' name."
    )
    private Property<List<String>> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        List<String> from = runContext.render(this.from).asList(String.class);

        LoadJobConfiguration.Builder builder = LoadJobConfiguration
            .newBuilder(BigQueryService.tableId(runContext.render(this.destinationTable).as(String.class).orElse(null)), from);

        this.setOptions(builder, runContext);

        LoadJobConfiguration configuration = builder.build();
        logger.debug("Starting query\n{}", JacksonMapper.log(configuration));

        Job loadJob = this.waitForJob(logger, () -> connection.create(JobInfo.newBuilder(configuration)
            .setJobId(BigQueryService.jobId(runContext, this))
            .build()
        ), runContext);

        return this.outputs(runContext, configuration, loadJob);
    }
}
