package io.kestra.plugin.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
                    code = {
                            "from:",
                            "  - \"{{ outputs['avro-to-gcs'] }}\"",
                            "destinationTable: \"my_project.my_dataset.my_table\"",
                            "format: AVRO",
                            "avroOptions:",
                            "  useAvroLogicalTypes: true"
                    }
            ),
            @Example(
                    full = true,
                    title = "Load a csv file with a defined schema",
                    code = {
                            "- id: load_files_test",
                            "  type: io.kestra.plugin.gcp.bigquery.LoadFromGcs",
                            "  destinationTable: \"myDataset.myTable\"",
                            "  ignoreUnknownValues: true",
                            "  schema:",
                            "    fields:",
                            "      - name: colA",
                            "        type: STRING",
                            "      - name: colB",
                            "        type: NUMERIC",
                            "      - name: colC",
                            "        type: STRING",
                            "  format: CSV",
                            "  csvOptions:",
                            "    allowJaggedRows: true",
                            "    encoding: UTF-8",
                            "    fieldDelimiter: \",\"",
                            "  from:",
                            "  - gs://myBucket/myFile.csv",
                    }
            )
    }
)
@Schema(
    title = "Load data from GCS (Google Cloud Storage) to BigQuery"
)
public class LoadFromGcs extends AbstractLoad implements RunnableTask<AbstractLoad.Output> {
    @Schema(
        title = "Google Cloud Storage source data",
        description = "The fully-qualified URIs that point to source data in Google Cloud Storage (e.g." +
            " gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the" +
            " 'bucket' name."
    )
    @PluginProperty(dynamic = true)
    private List<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BigQuery connection = this.connection(runContext);
        Logger logger = runContext.logger();

        List<String> from = runContext.render(this.from);

        LoadJobConfiguration.Builder builder = LoadJobConfiguration
            .newBuilder(BigQueryService.tableId(runContext.render(this.destinationTable)), from);

        this.setOptions(builder, runContext);

        LoadJobConfiguration configuration = builder.build();
        logger.debug("Starting query\n{}", JacksonMapper.log(configuration));

        Job loadJob = this.waitForJob(logger, () -> connection.create(JobInfo.newBuilder(configuration)
            .setJobId(BigQueryService.jobId(runContext, this))
            .build()
        ));

        return this.outputs(runContext, configuration, loadJob);
    }
}
